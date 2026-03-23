# GlobalReopen 清理缺失 TablePartition 计划

## Overview

为 `eloqstore` 的 `GlobalReopenRequest` 增加“清理本地仍存在、但远程已不存在的 TablePartition”的能力：

- `StoreMode::StandbyReplica`：对比本地分区列表与远端分区列表，若本地存在但远端不存在，则补发 `TruncateRequest` 清理本地。
- `StoreMode::Cloud`：对本地所有分区发 `ReopenRequest`；当某个分区在 Cloud 上也不存在时（`Reopen` 返回 `KvError::NotFound`），在所有 `ReopenRequest` 完成后补发 `TruncateRequest` 清理本地。

实现目标是尽量复用现有 `HandleDropTableRequest` 的 `TruncateRequest` 调度逻辑，代码保持简洁、风险可控，并补齐测试覆盖。

## Current State Analysis

### 1) 现有 `HandleGlobalReopenRequest`

当前 `HandleGlobalReopenRequest` 的核心逻辑在 `src/eloq_store.cpp`：

- `StoreMode::StandbyReplica`
  - 通过 `ListStandbyPartitionRequest` 获取远端分区列表（内部调用 `StandbyService::ListRemotePartitions`）。
  - 用远端分区列表构造 `ReopenRequest`，只对这些分区执行 `Reopen`。
  - 对“本地存在但远端不存在”的分区，没有额外的清理动作。
- 非 `StandbyReplica`（包含 `StoreMode::Cloud`）
  - 直接遍历本地 `options_.store_path` 下的分区目录，构造并执行 `ReopenRequest`。
  - `Reopen` 若因为远端 manifest 缺失返回 `KvError::NotFound`，当前逻辑会把错误作为 `GlobalReopenRequest` 的最终错误返回；也不会触发 `TruncateRequest`。

因此现状无法满足“清理本地残留分区”的需求。

### 2) `TruncateRequest` 的清理语义

`TruncateRequest` 在 Shard 中会路由到 `BatchWriteTask::Truncate(trunc_pos)`（`src/storage/shard.cpp`）。

`BatchWriteTask::Truncate` 在 `trunc_pos.empty()` 时执行“全分区截断”：

- 复用 `IndexPageManager` 的 CowRoot
- 回收所有映射页（`FreePage(page_id)`）
- 将 `cow_meta_.root_id_`/`ttl_root_id_` 置为 `MaxPageId`
- `UpdateMeta()` 写回新的空根 manifest

这能确保该分区在后续读取中表现为“空/不存在”（不会再基于旧 root 映射读出数据）。

### 3) 用于识别“远端缺分区”的错误码

`ReopenTask::Reopen()` 主要调用 `IndexPageManager::InstallExternalSnapshot()`（`src/tasks/reopen_task.cpp` + `src/storage/index_page_manager.cpp`）。

在 Cloud 模式下，`InstallExternalSnapshot()` 会调用 `CloudStoreMgr::RefreshManifest()`：

- 如果 Cloud 上找不到 manifest（或表分区目录对应的 manifest 不存在），通常会返回 `KvError::NotFound`
- 该错误会沿调用链返回到 `ReopenRequest`，从而进入 `HandleGlobalReopenRequest` 的回调汇总逻辑

本计划将以 `KvError::NotFound` 作为 “Cloud 上也不存在该 TablePartition” 的判定条件。

## Desired End State

### A. StandbyReplica

当执行 `GlobalReopenRequest` 且 `Mode() == StoreMode::StandbyReplica` 时：

1. 仍然按现有方式获取远端分区列表。
2. 同时扫描本地 `options_.store_path` 下的分区目录，得到本地分区列表。
3. 计算集合差：`local_only = local_partitions - remote_partitions`
4. 对 `local_only` 中每个分区补发 `TruncateRequest`（`SetArgs(partition, {})`，即空 position 表示全截断）。
5. 最终 `GlobalReopenRequest` 完成时，返回值语义遵循：
   - 远端正常分区 `Reopen` 失败的错误会影响最终错误码
   - `local_only` 对应的 `TruncateRequest` 失败会影响最终错误码

### B. Cloud

当执行 `GlobalReopenRequest` 且 `Mode() == StoreMode::Cloud` 时：

1. 像现状一样对“本地分区集合”全部发起 `ReopenRequest`。
2. 等所有 `ReopenRequest` 完成后：
   - 收集所有 `ReopenRequest` 返回 `KvError::NotFound` 的分区集合
   - 对这些分区补发 `TruncateRequest`，清理本地残留
3. 最终 `GlobalReopenRequest` 完成时：
   - `Reopen` 返回 `KvError::NotFound` 的分区应当不再直接导致最终错误（因为我们会清理本地）
   - 但如果 `TruncateRequest` 也失败，则最终错误码需要反映该失败

## Key Discoveries:
- `src/eloq_store.cpp`：`HandleGlobalReopenRequest` 目前 `StandbyReplica` 仅重开远端分区，未做本地残留清理。
- `src/eloq_store.cpp`：`StoreMode::Cloud` 当前走“本地遍历 + Reopen 汇总错误”的逻辑分支，未对 `KvError::NotFound` 做二次清理。
- `src/storage/shard.cpp`：`RequestType::Truncate` 路由到 `BatchWriteTask::Truncate(trunc_req->position_)`。
- `src/tasks/batch_write_task.cpp`：`BatchWriteTask::Truncate` 在 `trunc_pos.empty()` 时执行全分区清理并 `UpdateMeta()`。
- `src/standby_service.cpp` + `src/storage/shard.cpp`：`ListStandbyPartitionRequest` 实际调用 `StandbyService::ListRemotePartitions`，远端分区列表基于远端 store 路径下目录（ls/ssh）结果。

## What We're NOT Doing

- 不引入新远端接口或改变 `Reopen`/`Truncate` 的底层实现语义。
- 不做复杂的“边执行边清理/两阶段并发”以免引入调度竞态；本计划采用“先 Reopen 汇总完成，再触发 Truncate”的顺序。
- 不改变 `partition_filter` 的语义：过滤条件仍同时作用于本地扫描与远端分区列表的收集/差集计算。

## Implementation Approach

### 总体策略：两阶段状态机

对 `HandleGlobalReopenRequest` 增加“两阶段”流程：

1. 阶段 1：对目标分区集合执行 `ReopenRequest`（保持现有并发调度模型）。
2. 阶段 2：当阶段 1 完成后，根据 `Mode()` 决定要不要发 `TruncateRequest`：
   - `StandbyReplica`：使用“本地分区差集（local_only）”作为截断目标
   - `Cloud`：使用阶段 1 中 `ReopenRequest` 返回 `KvError::NotFound` 的分区作为截断目标

阶段 2 的截断调度建议参照 `HandleDropTableRequest` 中的 `DropTableScheduleState` 代码风格（分页式 `ExecAsyn` + `pending_` 汇总）。

### StandbyReplica 细节

在 `Mode() == StoreMode::StandbyReplica` 分支中：

1. 保留现有的远端分区收集：
   - `ListStandbyPartitionRequest(&names)` -> `ListRemotePartitions`
2. 新增本地分区收集：
   - 遍历 `options_.store_path` 下目录名，`TableIdent::FromString(entry.filename())`
   - 过滤 `partition_filter`
3. 计算差集：
   - `remote_set`：远端分区（经过 filter）
   - `local_only`：本地分区 - 远端分区

然后：

- `partitions`（用于阶段 1 的 reopen）仍使用 `remote_partitions`（当前行为）
- `local_only` 作为阶段 2 的截断目标

### Cloud 细节

在 `Mode() == StoreMode::Cloud` 下：

- 阶段 1 的 reopen 分区集合仍是“本地分区目录集合”（现状）
- 阶段 1 的回调在遇到 `sub_err == KvError::NotFound` 时：
  - 不把该错误写入 `req->first_error_`（避免最终返回 `NotFound`）
  - missing 分区的判定推迟到“阶段 1 最后一个 reopen 回调（`pending_` 递减到 0）”里：该回调单线程遍历 `req->reopen_reqs_`，筛出 `Error()==KvError::NotFound` 的分区用于阶段 2 截断
- 阶段 1 完成后：
  - 若筛选出的 missing 分区非空，阶段 2 发起 `TruncateRequest`
  - 阶段 2 的截断错误才影响最终返回值

### 线程安全与去重

- 避免在回调中并发写共享容器（即避免 `std::mutex`）。
- 阶段 1 回调只做两件事：
  - 计算 `req->first_error_`（除 `KvError::NotFound` 外的错误）
  - 递减 `req->pending_`
- 阶段 1 最后一次回调中（当 `pending_` 递减到 0）：
  - 该回调单线程遍历 `req->reopen_reqs_`
  - 筛出 `reopen_req->Error() == KvError::NotFound` 的分区作为阶段 2 `TruncateRequest` 输入
- 去重策略：
  - 理论上 `reopen_reqs_` 来自“分区收集阶段”，如果收集阶段已保证唯一性则无需去重
  - 如仍担心重复，可在该单线程里用局部 `std::unordered_set` 去重（不引入锁）
- 关于你关心的“当前线程是否能看到其他 shard 写入的错误码”：依赖 `pending_` 的同步语义
  - `reopen_req->err_` 在 `KvRequest::SetDone()` 里先写入，再触发回调，回调里读取 `reopen_req->Error()` 能拿到正确值
  - 最后一次回调发生在所有其它回调都完成 `req->pending_.fetch_sub(..., std::memory_order_acq_rel)` 之后，因此最后回调读取其它子请求的 `Error()` 时应当可见
  - 关键前提：`pending_` 递减内存序需要保留为 `std::memory_order_acq_rel`（不要改成纯 `relaxed`）

### 并发度控制

- 阶段 2 的 `TruncateRequest` 并发度建议沿用 `options_.max_global_request_batch` 的上限策略（与 `HandleDropTableRequest` 一致）。

## Phase 1: Reopen 后补发 Truncate（核心逻辑）

### Overview

修改 `src/eloq_store.cpp::HandleGlobalReopenRequest`：

- 在 `StandbyReplica`：新增本地分区扫描并计算差集，阶段 2 截断本地-only 分区。
- 在 `Cloud`：对 `Reopen` 返回 `KvError::NotFound` 的分区记录缺失集合，阶段 2 截断缺失分区。

### Changes Required:

#### 1. `src/eloq_store.cpp`：增强 `HandleGlobalReopenRequest`
**File**: `src/eloq_store.cpp`
**Changes**:

- 扩展 `HandleGlobalReopenRequest` 的内部状态机：
  - 增加截断目标集合（`local_only` 或 `cloud_missing`）
  - 在阶段 1 完成后触发阶段 2 调度
- 修改回调汇总策略：
  - `Cloud` 模式下 `Reopen` 的 `KvError::NotFound` 不再直接写入 `req->first_error_`

#### 代码骨架（示意）

```cpp
// PSEUDO-CODE: 仅展示结构，不是最终可编译代码

if (Mode() == StoreMode::StandbyReplica) {
  // remote_partitions: ListStandbyPartitionRequest
  // local_partitions: scan options_.store_path directories
  partitions = remote_partitions; // reopen targets
  local_only = local_partitions - remote_partitions; // truncate targets
}
else {
  partitions = local_partitions; // reopen targets
  local_only = {}; // not used in Cloud path
}

Schedule ReopenRequest for partitions with callbacks:
  on_reopen_done(reopen_req):
    if err != NoError:
      if !(Mode()==Cloud && err==NotFound):
         // NotFound in Cloud means "missing on cloud", ignore here and
         // handle in phase 2.
         update req->first_error_

    if last reopen finished:
      // Phase 2: schedule truncate (if any) and merge errors.
      std::vector<TableIdent> truncate_targets;
      if Mode()==Cloud:
        // Single-thread phase-2 target selection:
        // scan req->reopen_reqs_ and collect sub-reopen requests that
        // returned KvError::NotFound.
        for sub in req->reopen_reqs_:
          if sub->Error() == KvError::NotFound:
             truncate_targets.push_back(sub->TableId())
      else:
        // StandbyReplica: phase-2 target is precomputed local_only.
        truncate_targets = local_only

      if truncate_targets.empty():
         req->SetDone(req->first_error_)
         return

      // Reuse req->pending_ / req->first_error_ for truncate aggregation.
      req->pending_.store(truncate_targets.size(), relaxed)
      // first_error_ currently holds reopen aggregation result (excluding
      // Cloud NotFound). truncate errors will be merged into it.

      Schedule TruncateRequest for each partition in truncate_targets:
        on_truncate_done(sub_trunc_req):
          if sub_trunc_req->Error() != NoError:
             update req->first_error_
          if last truncate finished:
             req->SetDone(req->first_error_)
```

### Success Criteria:

#### Automated Verification:
- [x] `ctest` 或项目现有测试命令通过（至少跑 `tests/cloud.cpp` 与 `tests/standby.cpp` 相关用例）
- [x] 新增/修改的测试用例通过，并覆盖：
  - Cloud 模式：本地残留分区在远端缺失后会触发 `TruncateRequest`
  - StandbyReplica：本地残留分区在远端缺失后会触发 `TruncateRequest`

本轮实现已在当前环境完成的验证：
- 编译通过（`ninja`）
- `tests/standby.cpp`：新增用例 `standby global reopen truncates local-only partitions` 已通过；同时原有 `standby rsync replica follows master changes` 也通过
- `tests/cloud.cpp`：`cloud global reopen truncates missing partitions` 已通过；且 `./cloud "[cloud][reopen]"` 整体通过（MinIO 可达）

#### Manual Verification:
- [ ] 在压测/集成环境的 `GlobalReopenRequest` 场景下，确认不会因为 `KvError::NotFound` 导致全局请求失败（前提是截断成功）
- [ ] 检查日志中是否能观察到“哪些分区触发了 truncate”的清晰输出（避免排障困难）

---

## Phase 2: 补齐测试用例

### Overview

新增测试覆盖“远端缺分区 => 本地 truncate”。

### Changes Required:

#### 1. `tests/cloud.cpp`：新增 CloudMode 缺分区清理用例
**File**: `tests/cloud.cpp`
**Changes**:

- 新增 `TEST_CASE`：
  - 初始化 cloud mode store
  - 让本地存在至少两个分区（A、B），但 Cloud 上仅保留分区 A（删除 B 的远端目录/manifest）
  - 保持本地分区 B 残留存在
  - 执行 `GlobalReopenRequest`
  - 验证：
    - 分区 B 的读取返回 `KvError::NotFound`
    - 分区目录下 data files 被清理（如果现有 truncate 路径触发了清理；至少需要确保业务读语义正确）

#### 2. `tests/standby.cpp`：新增 StandbyReplica 本地残留清理用例
**File**: `tests/standby.cpp`
**Changes**:

- 新增 `TEST_CASE`：
  - 初始化 StandbyReplica store（本地 store_path + standby_master_store_paths 作为“远端”目录）
  - 让 master 上创建分区 A 与 B
  - 让 standby 触发同步/加载，使本地也生成 A 与 B 的分区目录（可通过 `Scan` 或写入读取触发逻辑）
  - 手动删除 master 的分区 B 目录（使远端缺失）
  - 执行 `GlobalReopenRequest`
  - 验证：
    - standby 上分区 B 读取返回 `KvError::NotFound`
    - （可选）等待目录中 data files 被回收/清理

### Success Criteria:

#### Automated Verification:
- [x] 新增用例在本地通过（云端 MinIO 可达 + standby 本地模式）
- [x] 无编译警告/未处理的未使用变量（`ninja` + `ReadLints`）

#### Manual Verification:
- [ ] 如 truncate 清理是异步回收，确认验证条件（例如“等待 data file 消失”）不会引入过长超时时间

---

## Phase 3: 验证与收尾

### Success Criteria:

#### Automated Verification:
- [x] 运行全量或至少相关集成测试（`./cloud "[cloud][reopen]"` 与 `./standby "[standby]"`）
- [ ] 观察新逻辑下 `GlobalReopenRequest` 的最终返回码：
  - Cloud 模式：远端 missing 的分区不会导致整体返回 `KvError::NotFound`（若 truncate 成功）
  - StandbyReplica：远端 missing 的分区会被 truncate 后整体返回 `NoError`（若无其它错误）

#### Manual Verification:
- [ ] 在日志层面确认定位信息足够（如“truncate partition X because cloud missing/not in remote list”）

## References

- `src/eloq_store.cpp`：`HandleGlobalReopenRequest` 现有实现与请求调度逻辑
- `src/tasks/batch_write_task.cpp`：`BatchWriteTask::Truncate` 全分区截断语义（`trunc_pos.empty()`）
- `src/standby_service.cpp`：`StandbyService::ListRemotePartitions` 的远端分区收集方式
- `src/storage/shard.cpp`：`RequestType::Truncate` / `RequestType::ListStandbyPartition` 路由关系

