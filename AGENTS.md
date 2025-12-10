# Repository Guidelines

## Project Structure & Module Organization
- Core store code lives at repo root (`*.cpp`/`*.h`) and compiles into the `eloqstore` static library defined in `CMakeLists.txt`.
- Support modules: `tests/` (Catch2-based suites), `examples/` (usage demos), `benchmark/` and `micro-bench/` (performance tools), `db_stress/` (stress harness), `external/` (third-party helpers), and in-tree deps `abseil/`, `concurrentqueue/`, `inih/`.
- Generated artifacts belong in `build/`, `build_coverage/`, or a custom `cmake -B <dir>` output directory; keep source tree clean.

## Build, Test, and Development Commands
- Configure + build (debug+asan): `cmake -B build -DWITH_ASAN=ON -DWITH_UNIT_TESTS=ON` then `cmake --build build -j$(nproc)`.
- Release build: `cmake -B build-release -DCMAKE_BUILD_TYPE=Release` followed by `cmake --build build-release -j$(nproc)`.
- Run unit tests: `ctest --test-dir build/tests` after building with `WITH_UNIT_TESTS`.
- Coverage workflow: `tests/test_unit_coverage.sh` (requires `lcov`/`genhtml`) generates `build_coverage/unit_test_coverage_report/index.html`.
- Benchmarks: `./Release/benchmark/load_bench --kvoptions <path>` once the Release tree is built.

## Coding Style & Naming Conventions
- Apply `.clang-format` (Google base, 4-space indent, Allman braces, right-aligned pointers) before submitting; run `clang-format -i <files>`.
- Keep filenames snake_case; classes and structs use PascalCase, functions and variables stay lower_snake_case as in existing code.
- Prefer `#include` order matching the configured categories (local headers last) and guard new headers with `#pragma once`.

## Testing Guidelines
- Unit tests rely on Catch2; mirror `tests/*.cpp` by feature (`scan.cpp`, `persist.cpp`, etc.).
- Name new executables by feature (`foo.cpp` → target `foo`) and register via `tests/CMakeLists.txt`.
- Maintain coverage by extending existing suites and running `ctest`; add integration/benchmark checks when touching performance code.

## Commit & Pull Request Guidelines
- Follow existing history: short, imperative subject with optional context and PR number (e.g., `fix manifest flush (#123)`).
- Each PR should describe the change, its motivation, testing (`ctest`, coverage script, benchmarks), and link to any tracking issue.
- Include configuration notes (e.g., ASAN, WITH_COVERAGE) and relevant metrics or screenshots for UI/benchmark output.

## Security & Configuration Tips
- `CMakeLists.txt` auto-initializes submodules; ensure `git submodule update --init --recursive` before first build.
- Keep `ELOQ_MODULE_ENABLED` and data-path options in sync with EloqKV deployments; document any new config keys in `README.md`.




# 项目大体算法思想与架构
注意是使用cmakelist来编译的,要进入build目录执行编译,然后cloud单元测试路径是build/tests/cloud,完整的单元测试是ctest --test-dir /tests/,我来简单介绍一下这个eloqstore的kv存储引擎,ElogStore采用COWB+Tree数据结构以实现稳定低延迟的读与高效的批量写COW使得读操作无须加锁,组合boost coroutine与io_uring以实现高效并发并充分利用 NVMe SSD。实现了coroutine执行调度与并发控制。支持磁盘顺序写与随机写两种模式'生成snapshot。支持cloud模式，同步存储到s3,本地SSD作为缓存,支持scan读和点读,和batchwrite,读事务和写事务都要获取当前mapping表的一份拷贝,这是他们的mvcc,写操作对于要修改的page,发生一次cow,在拷贝出来的页面上执行修改,修改完了之后将b+树上层的指针指向新的page,但是这里又要修改上层的indexpage的指针,所以又会触发对上层page的写,导致又要cow一份,导致递归向上的操作,为了避免这个问题,使用mapping映射,存放逻辑page_id到file_page_id的映射,此时修改了一个page之后,不需要修改上层的指针指向,因为logic_id没变,只需要在mapping中的file_page_id换成新的页面即可,当上层需要修改的有且仅有下层page数量变化了,比如page写满了导致新增page,或者page被删除了,才需要修改上层的指针,这个时候必须要触发cow,同时如果上层因为指针的增加,导致自身写满了也发生了page分裂,这个过程也会一路向上传递,传递到根节点了就会将树高加一,那么所有的读操作和写操作都会在操作前保留一份当前mapping的snapshot,同时在写操作完成之后,会触发meta数据的更新(调用updatemeta),将datapage落盘,manifest落盘,更新当前shard的meta数据,当读操作完成时,会将snapshot的引用计数-1,当引用计数为0时,表示当前这个版本的快照已经没人使用了,就可以回收这份快照,并且回收这份快照中标记为待释放的文件

我们有一个task任务池,本质上每个task都是一个协程,有taskMgr这个类,里面存放着四种任务,BatchWriteTask(eloqstore的写请求,单个写也要调用batch,同时truncate也是batchwrite类),BackgroundWrite用来做compact+gc,然后还有ReadTask和ScanTask两种任务
当client发起一个request的时候,处理线程从request队列中拿出来,可以用它从taskMgr当中获取一个task,然后开启coroutine去执行

shard是我们的处理线程实例,同时一个shard对应一个asyncIoMgr,同时对应一个IndexPageMgr,同时我们有table和partion用来做分区,但是实际上table0.0和table0.1,和table1.0在eloqstore看来没有区别他们都是三个partition而已,没有说table0.0和table0.1有什么关系,对每个parition的request会被取余然后均分到shard上去处理

shard会workloop中从requests_中拿出request去处理,同时每个partion有一个自己pending队列,用于存放对一个partiton的多个request,会串行处理,同时cleanTTL和compact是作为batchwrite的内部产生的任务,也会增加到对应的parition的pending队列,被串行处理

shard的workloop每次会先将上次循环中有的coroutine提交的sqe给submit,然后pollcomplete(非阻塞),每次都会resume对应的协程,然后处理完了之后就处理新请求启动新的协程
# 核心类介绍
eloqstore 是存储的主体,对外暴露的类
shard 是eloqstore用于内部处理的工作线程
asyncIoMgr 是每个shard会持有的一个管理io的类,每个shard持有一个
IndexMgr是管理B+树索引的类
taskMgr是管理任务池的类,是task的owner
objectstore是 eloqstore和s3交互的工具类
filegc 是用于append mode下处理compact产生的冗余文件回收类
rootmeta是manifest文件在程序中的实例
replayer是用来载入manifest文件到程序的工具  
pagemapper是用来缓解cow带来的递归cow的问题的验证表
batchwrite是主要的写操作,还包含truncate等
archive_crond是用来生成启动归档task的累
back_ground_write 是用来处理后台任务的类,主要是compact和archive
test_utils 是用来单元测试的工具

# append模式和原地模式
这两种模式下的FilePageAllocator是不同的分配,append模式下是AppendAllocator,非append模式下是PooledFilePages,这个东西会复用,原地模式的旧的文件页会被重用,append模式下会发生gc来回收
WriteTask::AllocatePage中原地模式会调用WriteTask::AllocatePage,把filePage加入到mapping->to_free_file_pages_,然后后续FreeMappingSnapshot的时候给他放回池内再次复用

append模式下无论如何都是++max_file_page_id,不会重用已经释放的id,但是non append模式下他会维护一个"已经释放Id"的池子,优先池子中重用已经释放的id,当池子为空的时候才分配新的递增id,并且他是在WriteTask::FreePage当中,给"old_mapping_",然后mappingsnapshot析构的时候,就会给他回收进池子,这个设计主要是为了防止有读事务还在使用的时候就把他复用了,而append模式不会复用所以没有这个设计,不会用到之前那个

然后这个maxfileId会被写入manifest持久化,通过Replayer::DeserializeSnapshot和RePlayLog来恢复maxFileId

然后如果原地写模式下,这个释放池是没有持久化到manifest,所以每次重启的时候replay的时候都会用一个集合去遍历,如果某些id比max小,同时他没有被使用,则把他加入释放池

append模式会执行compact,当compact的时候会调用UpdateStat来更新appendAllocator的信息,当传入maxfileid(当前正在要写入的文件id)的时候,就会把当前"最小文件id"设置为"最大文件id",然后把空洞设置为0,表示将最小文件id移动到最大文件id的位置,然后中间就设置为没有空洞


# cloud模式和local模式
我现在这个kvstore存储,他有cloud模式和本地存储模式,如果是cloud模式下,本地的存储就变成缓存,对文件执行读写的时候,就是先看看本地缓存有没有文件,如果本地有文件则读本地,否则就调用rclone将云端下载到本地之后,再尝试读本地,执行写操作的话,也是先对本地文件执行fsync落盘,然后同时上传云端,上传成功了之后,才认为这一次写成功了,同时我们是使用追加写模式的,即对每个page操作时候会拷贝一份然后追加,那么就会导致前面部分有些page是无效的,同时有一个file_amplifier_factor作为放大因子,如果一个文件当中的有效page数量少于这个放大因子的倒数,则发生compact,将里面残存的零星page,执行一次拷贝并追加,同时从manifest中将这个文件去除引用,然后等待后续的gc线程回收文件,同时我们只有batchwrite这一种写操作,为了提高性能,同时仅在batchwrite中会触发是否要compact的判断,具体是在updateMeta的时候触发 #,如果需要compact,则会发送任务给shard的pending_queue,然后shard下一轮协程调度过来的时候就处理这个数据搬迁工作,然后做完之后triggerGC,将任务发送给后台gc线程,然后gc线程遍历manifest文件,统计哪些文件被引用,然后删除目录下的没有被引用的文件,大概是这么一个执行逻辑,流程如下-
batchwrite → updateMeta → CompactIfNeeded （判断并添加到pending队列）,注意这个地方没有执行什么操作,仅仅是调用shard->AddPendingCompact(tbl_ident_)添加到shard的compact任务

shard调度 → backgroundWrite → CompactDataFile （实际压缩搬迁数据）

压缩完成 → TriggerFileGC （触发垃圾回收）,local模式下,内部直接调用eloq_store->file_gc_->AddTask()来发给gc线程,cloud模式下直接发起异步http请求给rclone server处理minio回收,triggerfileGC内部会先收集所有被引用的file_id,,然后发送任务

后台GC线程 → 删除无引用文件 （实际回收空间）,以上是本地gc的流程,本地gc因为要调用list的操作,这是个同步接口,为了避免阻塞当前协程所以不打算使用当前协程内执行,所以发送到线程池去做
现在有云端gc,鉴于是云端gc,所以本地的磁盘其实变成了缓存,所以换成云端gc后,就不是对本地的文件判断compactifneeded了,而是要从远程查看远程的是否需要compact,也就是需要先下载到本地,同时理逻辑应该变成,先把远程拉下来判断是否需要compact,然后给shard发compact请求,搬迁数据,搬迁完之后然后把新的写到本地和同步远程,此时发起gc请求给gc线程,由他去minio当中list数据,查看有那些文件,哪些没有被manifest引用的文件,这里要包括历史的archive的manifest的所引用的文件,然后对远程执行删除,由于这里都是异步发起http请求的,所以这个地方无论是delete还是list都是异步接口,所以可以放到协程当中优化掉线程池

上文提到本地的lru成为了缓存,具体的参数是由local_space_limit来限制,cloudMgr当中有一个FileCleaner类,里面是用来执行lru的驱逐,cloud模式下打开文件,新创建文件,都需要执行ReserveCacheSpace,如果"当前已经使用的"+"当前需要分配的">"local_space_limit"就会触发lru回收,知道回收够了足够的空间,并且这里回收的都是已经没有被使用的fd,如果没有空闲的fd(都被打开),会导致没有东西可以回收,导致写不进去所以fd_limit不能开的太大

# fdlimit机制和LruFd
同时我们有fdlimit机制,用来提前注册一些fd,但是这里需要注意一下,lru缓存删除文件仅仅删除那些没有被打开的文件(即使这个文件还是有效的,此时cloud会存一份),所以一旦fdlimit设置的太大了,就会导致同时打开的文件数量太多,比如同时允许打开20G文件,但是本地lru只有10G,就会导致没有可以回收的缓存导致本地缓存爆了

所以fdlimit是用于提前通过iouring向内核提前注册一批fd,防止过程中的频繁系统调用,这个fdlimit也是通过lru来管理,如果当前需要继续使用新的fd,但是fdlimit已经到达上限了,就会尝试syncfile一些文件,然后把他们的fd让出来使用

因为是协程,可能会存在不同的协程同时尝试open和close一个fd,所以当open和close的时候需要上锁,确保只能有一个协程提交这次操作,注意这里的上锁不是std::mutex或者原子变量这种,单线程下的协程模型不需要考虑同时竞争的情况

所有权关系
LruFd的owner是PartitionFiles里面来存储LruFd
ioUringMgr是PartitionFiles的owner
任何需要访问文件描述符的代码都是Ref的owner(如WriteReq内部类,Manifest),当所有持有的Ref的代码都释放了Ref的时候,LruFd的ref_count_就会减到0然后触发Clear,将这个Lrufd进入lru头部

这个Ref是用来增加/减少一个LruFd的引用计数类,当代码要使用Lrufd的时候,都要获取Fd,当ref_count从0->1时候,从Lru链表中移除(移除在Ref的普通构造函数中,拷贝构造只是把计数增加),当回到0时又加入Lru,表示可以被淘汰

但是这里也是一个懒删除的设计,当没人引用这个fd了,只是把他放到lru当中,并没有回收他,如果当引用计数减到0了,同时他已经被关闭了,那么我们就可以把他回收了,当执行IouringMgr::CloseFile,不仅会 将他关闭,还会把他置为为-1表示empty,然后ref析构的时候发现cnt==0了,同时被关闭了,才会从哈希表里面回收
ps:FileId==0表示data_0,

# boost::context和task
我们的task本质上就是一个coroutine实例,提供了一些方法,resume,yield,waitIo等,boost::context虽然是一个对称协程(即调用方和被调用方平等,都互相持有对方上下文),但是我们的设计上还是非对称协程的设计,就是shard作为caller,然后当task->yield的时候,就是将cpu交还给shard(每个task的对方都是shard),然后当task->resume的时候(在ioMgr的pollcomplete),其实是给shard的ready_task.enqueue,然后在shard的ExecuteReadyTasks逐个唤醒去执行,当task执行完了(TaskStatus::Finished)之后会调用OnTaskFinished执行task_mgr_.FreeTask,回收会task_mgr



# manifest和snapshot和replayer和manifestbuilder
PageMapper是映射表的封装,持有一份mappingSnapShot的shared_ptr,mappingSnapShot是他的成员,snapShot才存放了实际上的映射表,同时PageMapper又是RootMeta的unique成员(也是CowRootMeta成员,合理)

但是rootMeta的成员还有mappingSnapShot的set,并且他是owner,记录当前root还引用了哪些快照,同时rootMeta还持有一份unique_ptr的PageMapper,这个才是rootMeta真正维护的东西,其他都只是信息而已,所以CowRootMeta也只是把这个把这个pageMapper拷贝一份,同时附带上他Cow那一刻的mappingSnapshot,可能是为了防止旧的mappingsnapshot没有人引用

shard->IndexManager()->MakeCowRoot只有在write的时候会触发,本质上就是传入一个CowRootMeta的引用进去,然后内部执行FindRoot然后拷贝上去
read都只需要调用shard->IndexManager()->FindRoot就可以,直接返回RootMeta,但是这里的RootMeta都是他调用FindRoot那一刻的快照

注意index_page_maneger当中会维护一个tbl_roots_,是每个tableident和他的rootMate的映射,使用Lazy Loading懒加载,只有第一次使用这个tableIdent的rootMeta的时候才会尝试加载,其他是否的FindRoot都是直接读

然后我们提供了一个replayer类用来载入manifest文件,把ManifestFile这个类传入即可,manifest文件本质上是由一条一条的manifest记录组成的,每个blob记录都有一个checksum,RePlay的时候会逐条记录解析,并且检验每条blob的checksum

manifest的格式为[Snapshot记录] + [Log记录1] + [Log记录2] + ... + [Log记录N]
有两种写入模式,一种是blob追加模式,使用manifestbuilder来实现,在WriteTask::FlushManifest中,会尝试将新的映射表的更新信息写入manifest,当manifest文件还没有超过大小的时候,采用追加的模式,当超过大小的时候,就会一次性把log记录全部压缩回snapshot当中,然后重新建立新的manifest文件,然后会先写一个临时tmp文件,然后rename原子切换过去然后对目录执行Fdatasync,更新里面的目录项

MappingSnapshot可以理解为一个 版本链表 （被shared_ptr管理），RootMeta维护着 当前活跃的版本 ，而CowRootMeta是 写操作的工作副本 。
- 写操作时 : CowRootMeta创建新的MappingSnapshot版本，写完后RootMeta前进到这个新版本
- 读操作时 : 读者获取到来那一刻RootMeta指向的MappingSnapshot的shared_ptr副本，形成快照隔离
- Pin机制 : 每当有新的引用者（MappingSnapshot、MemIndexPage）时就Pin()，引用者销毁时就Unpin()
- 垃圾回收 : 当ref_cnt_==0时，说明没有任何引用者，可以安全回收RootMeta

FindRoot在本地manifest没找到的时候,会返回notfound,如果是ReadTask则会直接返回FindRoot的结果(notfound),如果是writeTask,他是通过MakeCowRoot来调用的FindRoot,如果FindRoot返回了NotFound,则会直接在内存创建一个stub Rootmeta,创建一个空的PageMapper

后续刷盘的时候,是先刷data,然后调用FlushManifest,如果manifest_size == 0表示是第一次创建manifest,则会调用switchManifest创建完整快照,否则是调用AppendManifest来追加日志
ps:云端模式下可能看见data0,data6,但是data1和manifest都不存在,应该是合理的,因为传data是异步的,所以data1-data5还没传成功,manifest要等这些传完了才行

# indexPage和B+树相关
IndexStackEntry是write_tree_stack.h当中提供的一个索引栈(意味着只有写操作会使用),里面存放着MemIndexPage索引页指针,IndexPageIter该页的迭代器,在applyBatch当中,会先初始化索引栈用来遍历索引B+树

seekstask是用来优化B+树遍历的,对每一个key寻找他的叶节点索引的时候,不是每次都需要从顶部下去,这个函数用来回退到合适的层级,也就是当前层级如果包含了所需的key就停止回溯,然后通过BatchWriteTask::Seek往下遍历到当前key真实所需的叶节点

BatchWriteTask::Seek内部会调用IndexPageIter::Seek去查找单个page的具体位置,BatchWriteTask::Seek内部是while循环遍历B+树到根节点

IndexPageManager::FindPage提供了一个Swizzling机制,使用64位整数的低3位来编码区分SwizzlingPointer = 0,FilePageId=1,PageId=2,也就是把FillePageId直接左移三位,就可以使得低三位是0,采用动态加载更新,并且因为只有几个比特位的变化,所以FilePageId变成SwizzlingPointer并不会丢失信息,此时memIndexPage加载到内存中的时候,就可以直接把映射表当中记录的设置为Swizzling,就不需要读磁盘了,直接转化内存
ps:就是直接把FilePageId强转为指针然后存储,调用AddSwizzling和Unswizzling来操作

生命周期问题:
IndexPage是先通过创建memIndexPage然后修改完了之后调用FlushIndexPage然后里面调用WritePage执行刷盘才生成的indexPage
AllocIndexPage做的事情其实很简单,就只是创建一个memindexpage(无效的),无论是新创建的还是从free当中拿的(可能会触发recycle)
FindPage会先查看他是否是Swizzling,如果是就直接从内存中访问,否则就先AllocIndexPage一个然后触发readPage写上去,然后加入到mapping映射(只是从indexpage变成memindexpage而已),同时FindPage有一个并发访问的设计,因为这里有异步读取,所以会上锁,然后等异步读取完成之后wakeALL,然后while循环再走一轮,此时就已经是swizzling了,然后他之前被wait的就会进到最下面的else语句,然后再执行一次入队了(合理,因为要更新lru热度)
ps:有个问题,好像IndexPageManager::FindPage里面的FinishIo重复AddSwizzling了

注意:mapping当中的映射创建和磁盘是强相关的,磁盘中有,所以WritePage的AllocatePage(注意没有index)当中才更新cow的映射,allocIndexPage只是分配一个memindexpage(无效),但是AllocatePage才是会加入映射

然后当deleteTree的时候,会调用FreePage从内存中删掉(只删当前cow的mapper的),但是memindexpage还会保存在indexMgr里面(合理,因为可能还有读者要使用这个,这个memIndexPage的owner应该是indexMgr,只是mapping借用一下)

ps:FindRoot和FindPage不一样,FindRoot是有可能找不到manifest的但是FindPage调用的时候,已经保证了有Page存在于映射表当中了,因为他是通过上层的树找下来的

所以当前还是存在一个问题,就是有memIndexPage还没被evict所以rootMeta引用计数减不到0,但是这个时候他的mapping映射可能已经不记录任何的page了,因为此时已经没有任何的dataPage了包括indexPage,但是还是有memIndexPage(owner是IndexMgr)


# batchwrite实现
batchwrite和其他的task有一个区别就是,他会有一个cowrootmeta的成员,会在一开始的apply当中被makeCowRootMeta赋值,然后进入applyBatch之后,就会对每一批进行索引树查找,通过indexPage读取到pageid(逻辑),然后执行applyOnePage,这个函数会传入一个cidx,这个索引表示当前处理的data_batch的位置,它会在applyOnePage里面被前进,他会先获取当前的正在使用的page,然后获取到数据上限(change_end_it),然后我们就认为这次是applyOnePage这一轮的要处理的data_batch的上界,然后定义好迭代器开始前进执行三路归并,归并过程中调用add_to_page这个lambda执行插入page

三路归并的过程中可能会触发爆页的风险,把原先的数据挤掉了,add_to_page当中会处理Page Split然后执行Redistribute,最终归并结束了之后,会调用FinishDataPage来处理最终的数据页,内部会执行一些优化,如果当前是新page(包括老page split出来的),判断页面大小之类的然后执行redistribute,如果是旧page
ps:FinishDataPage是调用下文的ShiftLeafLink将前驱写入磁盘

同时如果没有新页面产生,就不需要修改indexpage,就只需要修改mapingsnapshot的映射(写操作的cow的映射),其他读操作的映射不变,还是可以正常访问page,同时由于cowRootMeta会保存一份cow_meta_.old_mapping_,在这个地方会标记old_mapping的一些映射为"待回收"状态,到时候old_mapping被释放掉之后,这些页面如果没有其他mapping引用的话,就可以回收了,然后新页面会执行stack_.back()->changes_.emplace_back把新增加的页面加入到IndexStackEntry当中

leaf_triple_是一个batchwrite三元组的局部缓存,0,1,2分别是前驱,当前和后驱节点,通过LoadTripleElement来载入,同时LoadApplyingPage的时候,如果三元组内部有就直接获取,同时ShiftLeafLink会执行将数据写入磁盘的功能(写入的是前驱节点leaf_triple[0],然后把leaf_triple[1]变成leaf_triple[0])

注意在pop的过程中,如果idx_page_builder_.IsEmpty()为真,则会调用FreePage来删除当前索引页,然后向上递归传播,如果整棵树都被删了,则root_id会被设置为maxPageId

# applyOnePage
是batchwrite的核心函数,一进去他就会判断stack_栈顶(当前遍历的位置)是否是有效的,如果是有效的则就把applying_page作为base_page,然后确认左右边界,创建迭代器并且让他前进一格(表示开始处理第一个元素),然后定义一个add_to_page的函数,他会先处理过期,过期就不要了(如果又过期,又发现他是溢出页指针,大物件要通知外仓回收)然后就尝试add插入,如果成功了就正常结束,如果失败了则要么是当前页不够位置了(把当前页给FinishDataPage),要么是溢出页要额外处理(通知外仓去存储,自己这里只存储指针)

然后定义一个AdvanceType,表示这一轮迭代器前进的方式,both表示两个指针都要前进
定义好add_to_page之后,开始归并排序,如果原先货物的编号小,就直接保留然后迭代器前进
如果编号(key)一样就要做取舍,查看更新时间,比如对一个key的多次更新,只保留最晚的一次更新
如果订单号更小,说明是新进仓库的kv,如果是upsert就正常写入,如果是delete就不管
如果归并有一方结束了,就直接把另一方全部弄进去就行
最后要处理一下最后一个箱子的情况,有可能最后一个箱子是空的,然后就要处理一些链接三元组的处理,索引树的处理,如果是有东西就正常刷入即可
ps:注意change_ts / base_ts这个是更新时间,expire_ts / base_expire是过期时间

# TTL机制
EloqStore确实采用了 双树结构 来实现TTL功能,数据表有一棵树,TTL有一棵树,数据树使用key来排序,TTL树采用时间戳排序,TTLKey编码格式为时间+数据,将过期时间以大端序编码放入,确保这个树按照过期时间排序

在applyBatch内部的applyOnePage的时候会调用updateTTL,将ttl信息加入到ttl_batch_,然后applyBatch执行完之后就会调用ApplyTTLBatch,内部就是对ttl_root_id_做applyBatch
ps:注意要先SetBatch(ttl_batch_)不然此时还是数据的batch
然后这个地方也是和数据的一样,都是要存kv,但是这里的v是空,但是遍历逻辑和数据书 一样,数据也是存放在datafile当中


# page类及其相关类
重启点和键压缩,我们对于key是有压缩操作,比如user001和user002,如果相邻的键存在公共前缀,我们就只需要存储差异部分,那么由于这样的压缩关系,导致后面的键依赖前面的键,导致要恢复一个键则需要恢复他之前的键,为了避免需要从页头开始恢复,于是使用了重启点,重启点会保存完好的数据(默认间隔16个),就可以从这个地方开始往后恢复,同时会在页面末尾存储所有重启键的偏移量数组,然后此时就是从重启点数组中开始二分查找



# 已完成优化设计
1.增加了字典压缩
压缩键流程
现在字典都被编码进manifest了,先写入字典长度,再追加字节数组,回访的时候根据长度读出完整的字典
引入值压缩：批量写入前对新 value 采样训练 Zstd 字典，被压缩的 value 按位标记写入数据页，并把字典持久化到 manifest 以便读取路径复原。

compression.cpp (line 25) compression.h (line 13) 新增 Zstd 字典压缩工具，含采样、训练、压缩/解压流程及大 value 的独立压缩路径。
batch_write_task.cpp (line 218) 写入前调用 SampleAndBuildDictionaryIfNeeded 并在 ApplyOnePage 中用 compression::Prepare 决定 value 的压缩方式，写入时在 ValLenBit 中编码压缩类型（data_page_builder.cpp (line 126)）。
data_page.cpp (line 338)/DecodeEntry 扩展 value 长度字段的位语义，DataPageIter::CompressionType 能返回压缩枚举，ResolveValue 负责溢出页+压缩解码（task.cpp (line 127)），被 ReadTask 与 ScanIterator 复用。
write_task.cpp (line 191)、background_write.cpp (line 220)、ManifestBuilder::Snapshot 将字典长度与字节写入 manifest，Replayer::DeserializeSnapshot 还原并交给 RootMeta::compression_（index_page_manager.cpp (line 173)）。
实现过程：写路径在获取 CowRootMeta 时得到共享的 DictCompression（index_page_manager.cpp (line 167)），对本批 value 采样并按阈值训练；每个变更通过 compression::Prepare 决定是否使用字典或独立压缩，压缩类型编码在数据页 value 长度的高位。读取、扫描通过 ResolveValue 判断位标记来解压或读溢出页。Manifest snapshot 增加字典长度与内容字段，归档和回放时恢复字典，保证重启后仍能解压历史数据。写完 manifest 的 snapshot 会重置压缩器的 “dirty” 状态，避免重复刷字典。
2.增加了cloud模式下的文件预热
现在由于cloud模式下,每次都要触发下载就很麻烦,所以其实可以先直接下载到本地文件上,而且应该调用list去查看有哪些目录,就知道有哪些partition了,然后就把文件下载下来之后并且打开加入lru.实现这个预热文件下载并且打开加入lru的逻辑,你需要关注一些信息,这个功能应该是可选的,要加入到kvoption里面,然后他应该要下载到local_space_limit这个数据(注意考虑预留空间,注意下载下来之后要把文件加入local_space_limit的lru管理,同时注意partition要对应上他们对应的shard,要加到对应的shard的partition,他是通过partitionid来计算这个partition应该落到哪个shard,所以说具体的下载接口应该要走的是openorcreatefd然后一路走下去,然后应该是由一个shard去做RequestType::ListObject来查看有哪些partition,这个返回之后,再给每个partition发送提前下载的请求(注意这个地方应该是启动eloqstore的主线程要做的,不是shard线程),然后注意下载的时候要先下载manifest,然后是下载fileid较大的那一批文件,这里是最新的应该也是最活跃的数据,注意每个shard的下载量不能超过每个shard被分到的local_space_limit

3.增加了在read page的时候一次多读几个页
scan任务的数据页预取,意思就是一次多返回一些
SeekIndexMultiplePages 在索引树上递归：先通过 FindPage 拿到当前索引页（node），用 IndexPageIter 定位到>=key的分支。如果当前节点还不是叶子，直接递归到子节点；到达叶子后，把命中的叶页ID放进 results，再顺着同级索引迭代，把后续兄弟叶页依次加入，直到达到 limit 或遇到 MaxPageId。
ScanIterator::PrefetchPages 利用上述接口预取连续叶页：先把逻辑 PageId 转换成物理 FilePageId，调用 IoMgr->ReadPages 批量读盘，组合成 (PageId, Page) 存到 prefetched_pages_，第一块直接成为活跃数据页，迭代器随之复位。Next() 时优先消费预取缓存，若缓存耗尽才回落到链表 NextPageId() 拉取下一页。
kv_options 新增 enable_compression，写入路径根据该开关决定是否对 value 做压缩。