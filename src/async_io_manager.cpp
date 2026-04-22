#include "async_io_manager.h"

#include <dirent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <linux/openat2.h>
#include <signal.h>
#include <spawn.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <boost/algorithm/string/predicate.hpp>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "error.h"
#include "replayer.h"
#include "tasks/write_task.h"
#include "utils.h"

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>
#include <bthread/eloq_module.h>
#endif

#include <butil/time.h>

#include "cloud_storage_service.h"
#include "coding.h"
#include "common.h"
#include "eloq_store.h"
#include "kill_point.h"
#include "kv_options.h"
#include "standby_service.h"
#include "storage/root_meta.h"
#include "storage/shard.h"
#include "tasks/task.h"
#include "tasks/write_task.h"

extern char **environ;

namespace eloqstore
{
namespace fs = std::filesystem;

namespace
{
constexpr std::string_view kManifestTmp = "manifest.tmp";
constexpr std::string_view kDirectoryEvictingPath = "<directory>";

WriteTask *CurrentWriteTask()
{
    KvTask *task = ThdTask();
    if (task == nullptr)
    {
        return nullptr;
    }
    switch (task->Type())
    {
    case TaskType::BatchWrite:
    case TaskType::BackgroundWrite:
        return static_cast<WriteTask *>(task);
    default:
        return nullptr;
    }
}

}  // namespace

char *VarPagePtr(const VarPage &page)
{
    char *ptr = nullptr;
    switch (VarPageType(page.index()))
    {
    case VarPageType::MemIndexPage:
        ptr = std::get<MemIndexPage::Handle>(page)->PagePtr();
        break;
    case VarPageType::DataPage:
        ptr = std::get<DataPage>(page).PagePtr();
        break;
    case VarPageType::OverflowPage:
        ptr = std::get<OverflowPage>(page).PagePtr();
        break;
    case VarPageType::Page:
        ptr = std::get<Page>(page).Ptr();
        break;
    }
    assert(!((uint64_t) ptr & (page_align - 1)));
    return ptr;
}

bool VarPageRegistered(const VarPage &page)
{
    switch (VarPageType(page.index()))
    {
    case VarPageType::MemIndexPage:
    {
        const MemIndexPage::Handle &handle =
            std::get<MemIndexPage::Handle>(page);
        return handle.Get() != nullptr && handle->IsRegistered();
    }
    case VarPageType::DataPage:
        return std::get<DataPage>(page).IsRegistered();
    case VarPageType::OverflowPage:
        return std::get<OverflowPage>(page).IsRegistered();
    case VarPageType::Page:
        return std::get<Page>(page).IsRegistered();
    }
    return false;
}

std::unique_ptr<AsyncIoManager> AsyncIoManager::Instance(const EloqStore *store,
                                                         uint32_t fd_limit)
{
    const KvOptions *opts = &store->Options();
    if (opts->store_path.empty())
    {
        return std::make_unique<MemStoreMgr>(opts);
    }
    StoreMode mode = store->Mode();
    std::unique_ptr<AsyncIoManager> mgr;
    switch (mode)
    {
    case StoreMode::Cloud:
        mgr = std::make_unique<CloudStoreMgr>(
            opts, fd_limit, store->CloudService());
        break;
    case StoreMode::StandbyReplica:
    case StoreMode::StandbyMaster:
        mgr = std::make_unique<StandbyStoreMgr>(opts, fd_limit);
        break;
    case StoreMode::Local:
    default:
        mgr = std::make_unique<IouringMgr>(opts, fd_limit);
        break;
    }
    return mgr;
}

bool AsyncIoManager::IsIdle()
{
    return true;
}

IouringMgr::IouringMgr(const KvOptions *opts, uint32_t fd_limit)
    : AsyncIoManager(opts), fd_limit_(fd_limit)
{
    memset(&ring_, 0, sizeof(ring_));
    lru_fd_head_.next_ = &lru_fd_tail_;
    lru_fd_tail_.prev_ = &lru_fd_head_;

    uint32_t pool_size = options_->max_inflight_write;
    write_req_pool_ = std::make_unique<WriteReqPool>(pool_size);
    merged_write_req_pool_ = std::make_unique<MergedWriteReqPool>(pool_size);
}

IouringMgr::~IouringMgr()
{
    if (ring_inited_)
    {
        if (buffers_registered_)
        {
            io_uring_unregister_buffers(&ring_);
            buffers_registered_ = false;
        }
        io_uring_unregister_files(&ring_);
        for (auto &[_, tbl] : tables_)
        {
            for (auto &[_, fd] : tbl.fds_)
            {
                if (fd.fd_ >= 0 && fd.reg_idx_ < 0)
                {
                    close(fd.fd_);
                }
            }
        }

        io_uring_queue_exit(&ring_);
    }
}

KvError IouringMgr::Init(Shard *shard)
{
    (void) shard;
    return KvError::NoError;
}

KvError IouringMgr::BootstrapRing(Shard *shard)
{
    if (ring_inited_)
    {
        return KvError::NoError;
    }

    io_uring_params params = {};
    params.flags |= (IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
                     IORING_SETUP_TASKRUN_FLAG);
    const uint32_t sq_size = options_->io_queue_size;
    int ret = io_uring_queue_init_params(sq_size, &ring_, &params);
    if (ret < 0)
    {
        LOG(ERROR) << "failed to initialize io queue: " << ret;
        return KvError::IoFail;
    }
    ring_inited_ = true;

    if (fd_limit_ > 0)
    {
        ret = io_uring_register_files_sparse(&ring_, fd_limit_);
        LOG(INFO) << "register files sparse with fd_limit " << fd_limit_
                  << " ret " << ret;
        if (ret < 0)
        {
            LOG(ERROR) << "failed to reserve register file slots: " << ret;
            io_uring_queue_exit(&ring_);
            ring_inited_ = false;
            return KvError::OpenFileLimit;
        }
        free_reg_slots_.reserve(fd_limit_);
    }
    else
    {
        LOG(ERROR) << "fd_limit is zero, cannot register fixed files";
        io_uring_queue_exit(&ring_);
        ring_inited_ = false;
        return KvError::OpenFileLimit;
    }

    bool registered_buffers = false;
    size_t page_size = options_->data_page_size;
    size_t pool_bytes = (options_->buffer_pool_size / page_size) * page_size;
    constexpr size_t kMaxRegisteredBytes = 1ull << 30;  // 1 GiB per iovec.
    registered_buf_base_ = nullptr;
    registered_buf_stride_ = 0;
    registered_buf_shift_ = 0;
    registered_buf_count_ = 0;
    registered_last_slice_size_ = 0;
    write_buf_.reset();
    write_buf_size_ = 0;
    write_buf_pool_size_ = 0;
    write_buf_count_ = 0;
    write_buf_index_base_ = 0;

    std::unique_ptr<char, decltype(&std::free)> page_buffer{nullptr,
                                                            &std::free};
    std::vector<iovec> iovecs;
    uint16_t page_iov_count = 0;

    if (pool_bytes > 0)
    {
        void *raw_ptr = nullptr;
        int aligned = posix_memalign(&raw_ptr, page_align, pool_bytes);
        if (aligned == 0 && raw_ptr != nullptr)
        {
            page_buffer.reset(static_cast<char *>(raw_ptr));
            memset(page_buffer.get(), 0, pool_bytes);
            size_t remaining = pool_bytes;
            size_t offset = 0;
            while (remaining > 0)
            {
                size_t chunk_size = std::min(remaining, kMaxRegisteredBytes);
                iovec iov = {.iov_base = page_buffer.get() + offset,
                             .iov_len = chunk_size};
                iovecs.push_back(iov);
                page_iov_count++;
                remaining -= chunk_size;
                offset += chunk_size;
            }
        }
        else
        {
            LOG(WARNING) << "posix_memalign failed for registered pool, error: "
                         << aligned;
        }
    }

    if (options_->data_append_mode && options_->write_buffer_ratio > 0.0 &&
        options_->write_buffer_size > 0 && options_->buffer_pool_size > 0)
    {
        size_t write_buf_size = options_->write_buffer_size;
        size_t write_pool_bytes = static_cast<size_t>(
            static_cast<double>(options_->buffer_pool_size) *
            options_->write_buffer_ratio);
        write_pool_bytes =
            std::min(write_pool_bytes, options_->buffer_pool_size);
        if (write_buf_size % page_align != 0 ||
            write_pool_bytes % page_align != 0)
        {
            size_t aligned_buf = (write_buf_size / page_align) * page_align;
            size_t aligned_pool = (write_pool_bytes / page_align) * page_align;
            LOG(WARNING) << "write buffer size not aligned, adjusted from "
                         << write_buf_size << "/" << write_pool_bytes << " to "
                         << aligned_buf << "/" << aligned_pool;
            write_buf_size = aligned_buf;
            write_pool_bytes = aligned_pool;
        }
        if (write_buf_size > 0 && write_pool_bytes >= write_buf_size)
        {
            void *raw_ptr = nullptr;
            int aligned =
                posix_memalign(&raw_ptr, page_align, write_pool_bytes);
            if (aligned == 0 && raw_ptr != nullptr)
            {
                write_buf_.reset(static_cast<char *>(raw_ptr));
                memset(write_buf_.get(), 0, write_pool_bytes);
                write_buf_size_ = write_buf_size;
                write_buf_pool_size_ = write_pool_bytes;
                write_buf_count_ =
                    static_cast<uint16_t>(write_pool_bytes / write_buf_size);
            }
            else
            {
                LOG(WARNING)
                    << "posix_memalign failed for write buffers, error: "
                    << aligned;
            }
        }
        else
        {
            LOG(WARNING)
                << "write_buffer_ratio is too small to allocate write buffers";
        }
    }

    if (write_buf_ != nullptr && write_buf_count_ > 0)
    {
        write_buf_index_base_ = static_cast<uint16_t>(iovecs.size());
        size_t offset = 0;
        for (uint16_t i = 0; i < write_buf_count_; ++i)
        {
            iovec iov = {.iov_base = write_buf_.get() + offset,
                         .iov_len = write_buf_size_};
            iovecs.push_back(iov);
            offset += write_buf_size_;
        }
        write_buf_slots_.clear();
        write_buf_slots_.resize(write_buf_count_);
        write_buf_free_ = nullptr;
        for (uint16_t i = 0; i < write_buf_count_; ++i)
        {
            WriteBufSlot *slot = &write_buf_slots_[i];
            slot->index = i;
            slot->next = write_buf_free_;
            write_buf_free_ = slot;
        }
    }

    if (!iovecs.empty())
    {
        if (iovecs.size() > std::numeric_limits<uint16_t>::max())
        {
            LOG(WARNING) << "too many registered buffer slices: "
                         << iovecs.size();
            ret = -1;
        }
        else
        {
            ret =
                io_uring_register_buffers(&ring_, iovecs.data(), iovecs.size());
        }
        if (ret < 0)
        {
            LOG(WARNING) << "failed to register buffers: " << ret
                         << ", falling back to unregistered buffers";
            write_buf_registered_ = false;
        }
        else
        {
            buffers_registered_ = true;
            registered_buffers = true;
            write_buf_registered_ =
                (write_buf_ != nullptr && write_buf_count_ > 0);
            if (page_buffer != nullptr)
            {
                registered_buf_base_ = page_buffer.get();
                registered_buf_stride_ = kMaxRegisteredBytes;
                registered_buf_shift_ = 30;
                registered_buf_count_ = page_iov_count;
                registered_last_slice_size_ =
                    registered_buf_count_ == 0
                        ? 0
                        : iovecs[page_iov_count - 1].iov_len;
                shard->PagePool()->Init(page_buffer.release(), pool_bytes);
            }
            else
            {
                registered_buf_base_ = nullptr;
                registered_buf_stride_ = 0;
                registered_buf_shift_ = 0;
                registered_buf_count_ = 0;
                registered_last_slice_size_ = 0;
                shard->PagePool()->Init();
            }
        }
    }
    else
    {
        write_buf_registered_ = false;
    }

    if (!registered_buffers)
    {
        registered_buf_base_ = nullptr;
        registered_buf_stride_ = 0;
        registered_buf_shift_ = 0;
        registered_buf_count_ = 0;
        registered_last_slice_size_ = 0;
        shard->PagePool()->Init();
    }

    return KvError::NoError;
}

char *IouringMgr::AcquireWriteBuffer(uint16_t &buf_index)
{
    while (write_buf_free_ == nullptr)
    {
        write_buf_waiting_.Wait(ThdTask());
    }
    WriteBufSlot *slot = write_buf_free_;
    write_buf_free_ = slot->next;
    buf_index = static_cast<uint16_t>(write_buf_index_base_ + slot->index);
    char *ptr = write_buf_.get() + (slot->index * write_buf_size_);
    return ptr;
}

void IouringMgr::ReleaseWriteBuffer(char *ptr, uint16_t buf_index)
{
    if (write_buf_ == nullptr || write_buf_size_ == 0)
    {
        return;
    }
    uint16_t local_index =
        static_cast<uint16_t>(buf_index - write_buf_index_base_);
    assert(local_index < write_buf_slots_.size());
    WriteBufSlot *slot = &write_buf_slots_[local_index];
    slot->next = write_buf_free_;
    write_buf_free_ = slot;
    write_buf_waiting_.WakeOne();
    (void) ptr;
}

void IouringMgr::InitBackgroundJob()
{
    if (ring_inited_)
    {
        return;
    }

    Shard *target_shard = shard;
    CHECK(target_shard != nullptr)
        << "Shard must be set before initializing io_uring";
    KvError err = BootstrapRing(target_shard);
    if (err != KvError::NoError)
    {
        LOG(FATAL) << "failed to initialize io queue in background thread: "
                   << ErrorString(err);
    }
}

std::pair<Page, KvError> IouringMgr::ReadPage(const TableIdent &tbl_id,
                                              FilePageId fp_id,
                                              Page page)
{
    auto [file_id, offset] = ConvFilePageId(fp_id);
    std::string branch_name;
    uint64_t term;
    if (!GetBranchNameAndTerm(tbl_id, file_id, branch_name, term))
    {
        LOG(WARNING) << "ReadPage missing branch/term for fp_id=" << fp_id
                     << ", file_id=" << file_id << ", shift="
                     << static_cast<uint32_t>(options_->pages_per_file_shift)
                     << " in table " << tbl_id;
        return {std::move(page), KvError::ResourceMissing};
    }
    auto [fd_ref, err] = OpenFD(tbl_id, file_id, true, branch_name, term);
    if (err == KvError::NotFound)
    {
        err = KvError::ResourceMissing;
    }
    if (err != KvError::NoError)
    {
        return {std::move(page), err};
    }

    auto read_page = [this, file_id](
                         FdIdx fd, size_t offset, Page &result) -> KvError
    {
        int res;
        do
        {
            io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
            if (fd.second)
            {
                sqe->flags |= IOSQE_FIXED_FILE;
            }
            char *dst = result.Ptr();
            bool use_fixed = result.IsRegistered();
            if (use_fixed)
            {
                uint16_t buf_index = LookupRegisteredBufferIndex(dst);
                io_uring_prep_read_fixed(sqe,
                                         fd.first,
                                         dst,
                                         options_->data_page_size,
                                         offset,
                                         buf_index);
            }
            else
            {
                io_uring_prep_read(
                    sqe, fd.first, dst, options_->data_page_size, offset);
            }
            res = ThdTask()->WaitIoResult();
            if (res == 0)
            {
                LOG(ERROR) << "read page failed, reach end of file, file id:"
                           << file_id << ", offset:" << offset;
                return KvError::EndOfFile;
            }
            // retry if we read less than expected.
        } while ((res > 0 && res < options_->data_page_size) ||
                 ToKvError(res) == KvError::TryAgain);
        return ToKvError(res);
    };

    err = read_page(fd_ref.FdPair(), offset, page);
    if (err != KvError::NoError)
    {
        return {std::move(page), err};
    }

    if (!options_->skip_verify_checksum &&
        !ValidateChecksum({page.Ptr(), options_->data_page_size}))
    {
        LOG(ERROR) << "corrupted " << tbl_id << " page " << fp_id;
        return {std::move(page), KvError::Corrupted};
    }
    return {std::move(page), KvError::NoError};
}

KvError IouringMgr::ReadPages(const TableIdent &tbl_id,
                              std::span<FilePageId> page_ids,
                              std::vector<Page> &pages)
{
    assert(page_ids.size() <= max_read_pages_batch);

    struct ReadReq : BaseReq
    {
        ReadReq() = default;
        ReadReq(KvTask *task, LruFD::Ref fd, uint32_t offset)
            : BaseReq(task),
              offset_(offset),
              fd_ref_(std::move(fd)),
              page_(true) {};

        bool done_{false};
        uint32_t offset_;
        LruFD::Ref fd_ref_;
        Page page_{false};
    };

    // ReadReq is a temporary object, so we allocate it on stack.
    std::array<ReadReq, max_read_pages_batch> reqs_buf;
    std::span<ReadReq> reqs(reqs_buf.data(), page_ids.size());

    // Prepare requests.
    for (uint8_t i = 0; FilePageId fp_id : page_ids)
    {
        auto [file_id, offset] = ConvFilePageId(fp_id);
        std::string branch_name;
        uint64_t term;
        if (!GetBranchNameAndTerm(tbl_id, file_id, branch_name, term))
        {
            LOG(WARNING) << "ReadPages missing branch/term for file id "
                         << file_id << " in table " << tbl_id;
            return KvError::ResourceMissing;
        }
        auto [fd_ref, err] = OpenFD(tbl_id, file_id, true, branch_name, term);
        if (err == KvError::NotFound)
            err = KvError::ResourceMissing;
        CHECK_KV_ERR(err);
        reqs[i] = ReadReq(ThdTask(), std::move(fd_ref), offset);
        i++;
    }

    auto send_req = [this](ReadReq *req)
    {
        auto [fd, registered] = req->fd_ref_.FdPair();
        io_uring_sqe *sqe = GetSQE(UserDataType::BaseReq, req);
        if (registered)
        {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        char *dst = req->page_.Ptr();
        bool use_fixed = req->page_.IsRegistered();
        if (use_fixed)
        {
            uint16_t buf_index = LookupRegisteredBufferIndex(dst);
            io_uring_prep_read_fixed(sqe,
                                     fd,
                                     dst,
                                     options_->data_page_size,
                                     req->offset_,
                                     buf_index);
        }
        else
        {
            io_uring_prep_read(
                sqe, fd, dst, options_->data_page_size, req->offset_);
        }
    };

    // Send requests.
    while (true)
    {
        bool all_finished = true;
        for (ReadReq &req : reqs)
        {
            if (req.done_)
            {
                continue;
            }

            int res = req.res_;
            KvError err = ToKvError(res);
            if ((res >= 0 && res < options_->data_page_size) ||
                err == KvError::TryAgain)
            {
                // Try again.
                send_req(&req);
                all_finished = false;
            }
            else if (err != KvError::NoError)
            {
                // Wait for all of the inflight io requests to complete to avoid
                // PollComplete access invalid ReadReq* on this function stack
                // after returned.
                ThdTask()->WaitIo();
                return err;
            }
            else
            {
                // Successfully read this page.
                assert(res == options_->data_page_size);
                req.done_ = true;
            }
        }
        if (all_finished)
        {
            break;
        }
        // Retry until all requests are completed.
        ThdTask()->WaitIo();
    }

    // Validate result pages.
    if (!options_->skip_verify_checksum)
    {
        for (const ReadReq &req : reqs)
        {
            if (!ValidateChecksum({req.page_.Ptr(), options_->data_page_size}))
            {
                FileId file_id = req.fd_ref_.Get()->file_id_;
                LOG(ERROR) << "corrupted " << tbl_id << " file " << file_id
                           << " at " << req.offset_;
                return KvError::Corrupted;
            }
        }
    }

    pages.clear();
    pages.reserve(page_ids.size());
    for (ReadReq &req : reqs)
    {
        pages.emplace_back(std::move(req.page_));
    }
    return KvError::NoError;
}

std::pair<ManifestFilePtr, KvError> IouringMgr::GetManifest(
    const TableIdent &tbl_id)
{
    LruFD::Ref old_fd = GetOpenedFD(tbl_id, LruFD::kManifest);
    if (old_fd != nullptr)
    {
        assert(old_fd.Get()->ref_count_ == 1);
        CloseFile(std::move(old_fd));
    }

    std::string manifest_br = std::string(GetActiveBranch());
    uint64_t manifest_term = ProcessTerm();
    std::string manifest_name =
        BranchManifestFileName(manifest_br, manifest_term);
    auto [fd, err] =
        OpenFD(tbl_id, LruFD::kManifest, true, manifest_br, manifest_term);
    if (err != KvError::NoError)
    {
        return {nullptr, err};
    }
    struct statx result = {};
    auto [dir_fd, dir_err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    if (dir_err != KvError::NoError)
    {
        return {nullptr, dir_err};
    }
    int res = StatxAt(dir_fd.FdPair(), manifest_name.c_str(), &result);
    if (res < 0)
    {
        LOG(ERROR) << "failed to statx manifest file: " << strerror(-res);
        return {nullptr, ToKvError(res)};
    }
    uint64_t file_size = result.stx_size;
    assert(file_size > 0);
    auto manifest = std::make_unique<Manifest>(this, std::move(fd), file_size);
    return {std::move(manifest), KvError::NoError};
}

KvError IouringMgr::WritePage(const TableIdent &tbl_id,
                              VarPage page,
                              FilePageId file_page_id)
{
    auto [file_id, offset] = ConvFilePageId(file_page_id);
    uint64_t term = ProcessTerm();
    std::string_view branch = GetActiveBranch();
    auto [fd_ref, err] =
        OpenOrCreateFD(tbl_id, file_id, true, true, branch, term);
    CHECK_KV_ERR(err);
    fd_ref.Get()->dirty_ = true;
    TEST_KILL_POINT_WEIGHT("WritePage", 1000)

    auto [fd, registered] = fd_ref.FdPair();
    WriteReq *req = write_req_pool_->Alloc(std::move(fd_ref), std::move(page));
    io_uring_sqe *sqe = GetSQE(UserDataType::WriteReq, req);
    if (registered)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }

    char *ptr = req->PagePtr();
    if (VarPageRegistered(req->page_))
    {
        uint16_t buf_index = LookupRegisteredBufferIndex(ptr);
        io_uring_prep_write_fixed(
            sqe, fd, ptr, options_->data_page_size, offset, buf_index);
    }
    else
    {
        io_uring_prep_write(sqe, fd, ptr, options_->data_page_size, offset);
    }
    return KvError::NoError;
}

KvError IouringMgr::SubmitMergedWrite(const TableIdent &tbl_id,
                                      FileId file_id,
                                      uint64_t offset,
                                      char *buf_ptr,
                                      size_t bytes,
                                      uint16_t buf_index,
                                      std::vector<VarPage> &pages,
                                      std::vector<char *> &release_ptrs,
                                      std::vector<uint16_t> &release_indices,
                                      bool use_fixed)
{
    uint64_t term = ProcessTerm();
    std::string branch_str;
    GetBranchNameAndTerm(tbl_id, file_id, branch_str, term);
    std::string_view branch =
        branch_str.empty() ? GetActiveBranch() : branch_str;
    // In append mode, offset 0 means this merged write targets a brand-new
    // data file, so cloud mode can skip the remote not-found probe.
    const bool skip_cloud_lookup =
        options_->data_append_mode && !options_->cloud_store_path.empty() &&
        file_id <= LruFD::kMaxDataFile && offset == 0;
    OnFileRangeWritePrepared(tbl_id,
                             file_id,
                             branch,
                             term,
                             offset,
                             std::string_view(buf_ptr, bytes));
    auto [fd_ref, err] = OpenOrCreateFD(
        tbl_id, file_id, true, true, branch, term, skip_cloud_lookup);
    CHECK_KV_ERR(err);
    fd_ref.Get()->dirty_ = true;

    auto *req =
        merged_write_req_pool_->Alloc(static_cast<WriteTask *>(ThdTask()),
                                      std::move(fd_ref),
                                      buf_ptr,
                                      buf_index,
                                      bytes,
                                      offset,
                                      std::move(pages));
    req->release_ptrs_ = std::move(release_ptrs);
    req->release_indices_ = std::move(release_indices);
    req->use_fixed_ = use_fixed;

    if (!req->pages_.empty())
    {
        req->task_->inflight_io_ +=
            static_cast<uint32_t>(req->pages_.size() - 1);
    }

    io_uring_sqe *sqe = GetSQE(UserDataType::MergedWriteReq, req);
    auto [fd, registered] = req->fd_ref_.FdPair();
    if (registered)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    if (use_fixed)
    {
        io_uring_prep_write_fixed(
            sqe, fd, buf_ptr, bytes, static_cast<off_t>(offset), buf_index);
    }
    else
    {
        io_uring_prep_write(
            sqe, fd, buf_ptr, bytes, static_cast<off_t>(offset));
    }
    return KvError::NoError;
}

KvError IouringMgr::SyncData(const TableIdent &tbl_id)
{
    auto it_tbl = tables_.find(tbl_id);
    if (it_tbl == tables_.end())
    {
        return KvError::NoError;
    }

    // Scan all dirty files/directory.
    std::vector<FileId> dirty_ids;
    dirty_ids.reserve(32);
    for (auto &[file_id, fd] : it_tbl->second.fds_)
    {
        if (fd.dirty_)
        {
            dirty_ids.emplace_back(file_id);
        }
    }
    std::vector<LruFD::Ref> fds;
    fds.reserve(dirty_ids.size());
    for (FileId file_id : dirty_ids)
    {
        LruFD::Ref fd_ref = GetOpenedFD(tbl_id, file_id);
        if (fd_ref == nullptr)
        {
            continue;
        }
        fds.emplace_back(std::move(fd_ref));
    }
    return SyncFiles(tbl_id, fds);
}

KvError IouringMgr::AbortWrite(const TableIdent &tbl_id)
{
    // Wait all WriteReq finished to avoid PollComplete access this WriteTask
    // from WriteReq.task_ after aborted.
    ThdTask()->WaitIo();

    // Clear dirty flag on all LruFD.
    auto it_tbl = tables_.find(tbl_id);
    if (it_tbl == tables_.end())
    {
        return KvError::NoError;
    }
    for (auto &[id, fd] : it_tbl->second.fds_)
    {
        fd.dirty_ = false;
    }
    return KvError::NoError;
}

KvError IouringMgr::CloseFiles(const TableIdent &tbl_id,
                               const std::span<FileId> file_ids)
{
    if (file_ids.empty())
    {
        return KvError::NoError;
    }

    std::vector<LruFD::Ref> fd_refs;
    fd_refs.reserve(file_ids.size());
    for (FileId file_id : file_ids)
    {
        LruFD::Ref fd_ref = GetOpenedFD(tbl_id, file_id);
        if (fd_ref != nullptr)
        {
            fd_refs.emplace_back(std::move(fd_ref));
        }
    }

    if (fd_refs.empty())
    {
        return KvError::NoError;
    }

    return CloseFiles(std::span<LruFD::Ref>(fd_refs.data(), fd_refs.size()));
}

size_t IouringMgr::GetOpenFileCount() const
{
    return lru_fd_count_;
}

size_t IouringMgr::GetOpenFileLimit() const
{
    return fd_limit_;
}

KvError IouringMgr::TryCleanupLocalPartitionDir(const TableIdent &tbl_id)
{
    const std::string partition_name = tbl_id.ToString();

    LruFD::Ref dir_fd = GetOpenedFD(tbl_id, LruFD::kDirectory);
    const bool directory_active =
        dir_fd != nullptr && dir_fd.Get()->ref_count_ > 1;
    if (directory_active)
    {
        DLOG(INFO) << "Skip cleaning partition directory " << partition_name
                   << " because directory handle is still active";
        return KvError::NoError;
    }

    if (!StartEvictingPath(tbl_id, LruFD::kDirectory, 0))
    {
        DLOG(INFO) << "Skip cleaning partition directory " << partition_name
                   << " because directory cleanup is already in progress";
        return KvError::NoError;
    }

    auto close_idle_fd = [&](LruFD::Ref &fd_ref,
                             std::string_view fd_name) -> KvError
    {
        if (fd_ref == nullptr || fd_ref.Get()->ref_count_ != 1)
        {
            return KvError::NoError;
        }

        KvError close_err = CloseFile(std::move(fd_ref));
        if (close_err != KvError::NoError)
        {
            LOG(WARNING) << "Failed to close cached-idle " << fd_name
                         << " handle for table " << tbl_id << ": "
                         << ErrorString(close_err);
        }
        return close_err;
    };

    KvError close_err = close_idle_fd(dir_fd, "directory");
    if (close_err != KvError::NoError)
    {
        FinishEvictingPath(tbl_id, LruFD::kDirectory, 0);
        return close_err;
    }

    FdIdx root_fd = GetRootFD(tbl_id);
    int dir_res = UnlinkAt(root_fd, partition_name.c_str(), true);
    FinishEvictingPath(tbl_id, LruFD::kDirectory, 0);
    if (dir_res == 0 || dir_res == -ENOENT || dir_res == -ENOTEMPTY)
    {
        return KvError::NoError;
    }

    LOG(WARNING) << "Failed to delete partition directory " << partition_name
                 << " for table " << tbl_id << ": " << strerror(-dir_res);
    return ToKvError(dir_res);
}

KvError CloudStoreMgr::TryCleanupLocalPartitionDir(const TableIdent &tbl_id)
{
    const std::string partition_name = tbl_id.ToString();
    if (HasDirBusy(tbl_id))
    {
        DLOG(INFO) << "Skip cleaning partition directory " << partition_name
                   << " because directory is busy";
        return KvError::NoError;
    }

    if (HasTrackedLocalFiles(tbl_id))
    {
        DLOG(INFO) << "Skip cleaning partition directory " << partition_name
                   << " because local files are still tracked";
        return KvError::NoError;
    }

    return IouringMgr::TryCleanupLocalPartitionDir(tbl_id);
}

KvError IouringMgr::CleanManifest(const TableIdent &tbl_id)
{
    if (HasOtherFile(tbl_id))
    {
        DLOG(INFO) << "Skip cleaning manifest for " << tbl_id
                   << " because other files are present";
        return KvError::Busy;
    }

    uint64_t process_term = ProcessTerm();
    KvError dir_err = KvError::NoError;
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    dir_err = err;
    if (dir_err == KvError::NoError)
    {
        const std::string manifest_name =
            BranchManifestFileName(GetActiveBranch(), process_term);
        int res = UnlinkAt(dir_fd.FdPair(), manifest_name.c_str(), false);
        if (res < 0 && res != -ENOENT)
        {
            LOG(ERROR) << "Failed to delete manifest file for table " << tbl_id
                       << ": " << strerror(-res);
            KvError close_dir_err = CloseFile(std::move(dir_fd));
            if (close_dir_err != KvError::NoError)
            {
                LOG(WARNING) << "Failed to close directory handle for table "
                             << tbl_id << ": " << ErrorString(close_dir_err);
            }
            return ToKvError(res);
        }
        else if (res == 0)
        {
            DLOG(INFO) << "Successfully deleted manifest file for table "
                       << tbl_id;
        }

        KvError close_dir_err = CloseFile(std::move(dir_fd));
        if (close_dir_err != KvError::NoError)
        {
            LOG(WARNING) << "Failed to close directory handle for table "
                         << tbl_id << ": " << ErrorString(close_dir_err);
        }
    }
    if (dir_err != KvError::NoError && dir_err != KvError::NotFound)
    {
        LOG(ERROR) << "Failed to open directory for table " << tbl_id
                   << " during cleanup: " << ErrorString(dir_err);
        return dir_err;
    }
    KvError cleanup_err = TryCleanupLocalPartitionDir(tbl_id);
    if (cleanup_err != KvError::NoError)
    {
        LOG(WARNING) << "Failed to clean partition directory for table "
                     << tbl_id << ": " << ErrorString(cleanup_err);
        return cleanup_err;
    }
    return KvError::NoError;
}

KvError ToKvError(int err_no)
{
    if (err_no >= 0)
    {
        return KvError::NoError;
    }
    switch (err_no)
    {
    case -EPERM:
        return KvError::NoPermission;
    case -ENOENT:
        return KvError::NotFound;
    case -EINTR:
    case -EAGAIN:
    case -ENOBUFS:
        return KvError::TryAgain;
    case -ENOMEM:
        return KvError::OutOfMem;
    case -EBUSY:
        return KvError::Busy;
    case -EMFILE:
        return KvError::OpenFileLimit;
    case -ENOSPC:
        return KvError::OutOfSpace;
    default:
        LOG(ERROR) << "ToKvError: " << err_no;
        return KvError::IoFail;
    }
}

std::pair<void *, IouringMgr::UserDataType> IouringMgr::DecodeUserData(
    uint64_t user_data)
{
    UserDataType type = UserDataType(user_data);
    void *ptr = (void *) (user_data >> 8);
    return {ptr, type};
}

void IouringMgr::EncodeUserData(io_uring_sqe *sqe,
                                const void *ptr,
                                IouringMgr::UserDataType type)
{
    void *user_data = (void *) ((uint64_t(ptr) << 8) | uint64_t(type));
    io_uring_sqe_set_data(sqe, user_data);
}

IouringMgr::FdIdx IouringMgr::GetRootFD(const TableIdent &tbl_id)
{
    assert(!eloq_store->root_fds_.empty());
    const size_t num_roots = eloq_store->root_fds_.size();
    size_t disk_idx =
        tbl_id.StorePathIndex(num_roots, eloq_store->options_.store_path_lut);
    int root_fd = eloq_store->root_fds_[disk_idx];
    return {root_fd, false};
}

IouringMgr::LruFD::Ref IouringMgr::GetOpenedFD(const TableIdent &tbl_id,
                                               FileId file_id)
{
    auto it_tbl = tables_.find(tbl_id);
    if (it_tbl == tables_.end())
    {
        return nullptr;
    }
    auto it_fd = it_tbl->second.fds_.find(file_id);
    if (it_fd == it_tbl->second.fds_.end())
    {
        return nullptr;
    }
    // This file may be in the process of being closed.
    LruFD::Ref fd_ref(&it_fd->second, this);
    fd_ref.Get()->mu_.Lock();
    bool empty = (file_id == LruFD::kDirectory)
                     ? fd_ref.Get()->fd_ == LruFD::FdEmpty
                     : fd_ref.Get()->reg_idx_ < 0;
    fd_ref.Get()->mu_.Unlock();
    return empty ? nullptr : fd_ref;
}

std::pair<IouringMgr::LruFD::Ref, KvError> IouringMgr::OpenFD(
    const TableIdent &tbl_id,
    FileId file_id,
    bool direct,
    std::string_view branch_name,
    uint64_t term)
{
    return OpenOrCreateFD(tbl_id, file_id, direct, false, branch_name, term);
}

std::pair<IouringMgr::LruFD::Ref, KvError> IouringMgr::OpenOrCreateFD(
    const TableIdent &tbl_id,
    FileId file_id,
    bool direct,
    bool create,
    std::string_view branch_name,
    uint64_t term,
    bool skip_cloud_lookup)
{
    KvTask *current_task = ThdTask();
    if (current_task != nullptr && !current_task->ReadOnly())
    {
        auto *write_task = static_cast<WriteTask *>(current_task);
        if (write_task->NeedWaitDirEviction())
        {
            WaitForEvictingPath(tbl_id, LruFD::kDirectory, 0);
            write_task->ClearNeedWaitDirEviction();
        }
    }
    WaitForEvictingPath(tbl_id, file_id, term);
    auto [it_tbl, inserted] = tables_.try_emplace(tbl_id);
    if (inserted)
    {
        it_tbl->second.tbl_id_ = &it_tbl->first;
    }
    PartitionFiles *tbl = &it_tbl->second;
    auto [it_fd, _] = tbl->fds_.try_emplace(file_id, tbl, file_id, term);
    LruFD::Ref lru_fd(&it_fd->second, this);

    // Avoid multiple coroutines from concurrently opening or closing the same
    // file duplicately.
    lru_fd.Get()->mu_.Lock();
    if (file_id == LruFD::kDirectory)
    {
        if (lru_fd.Get()->fd_ != LruFD::FdEmpty)
        {
            lru_fd.Get()->mu_.Unlock();
            return {std::move(lru_fd), KvError::NoError};
        }
    }
    else if (lru_fd.Get()->reg_idx_ >= 0)
    {
        // Check for term or branch_name mismatch when not in local mode.
        if (eloq_store->Mode() != StoreMode::Local &&
            file_id != LruFD::kDirectory)
        {
            bool mismatch = false;
            // Check term mismatch (only when term is known).
            if (term != 0)
            {
                uint64_t cached_term = lru_fd.Get()->term_;
                if (cached_term != 0 && cached_term != term)
                {
                    mismatch = true;
                    DLOG(INFO) << "OpenOrCreateFD term mismatch, tbl=" << tbl_id
                               << " file_id=" << file_id
                               << " cached_term=" << cached_term
                               << " requested_term=" << term;
                }
            }
            // Check branch_name mismatch (always, regardless of term).
            if (!mismatch)
            {
                assert(!branch_name.empty());
                std::string_view cached_branch = lru_fd.Get()->branch_name_;
                if (!cached_branch.empty() && cached_branch != branch_name)
                {
                    mismatch = true;
                    DLOG(INFO)
                        << "OpenOrCreateFD branch mismatch, tbl=" << tbl_id
                        << " file_id=" << file_id
                        << " cached_branch=" << cached_branch
                        << " requested_branch=" << branch_name;
                }
            }

            if (mismatch)
            {
                // Mismatch detected, close and reopen with correct
                // term/branch.
                int old_idx = lru_fd.Get()->reg_idx_;
                int res = CloseDirect(old_idx);
                if (res < 0)
                {
                    lru_fd.Get()->mu_.Unlock();
                    return {nullptr, ToKvError(res)};
                }
                lru_fd.Get()->reg_idx_ = -1;
                // Fall through to open/create with correct term and branch
            }
            else
            {
                // No mismatch, use cached FD.
                lru_fd.Get()->mu_.Unlock();
                return {std::move(lru_fd), KvError::NoError};
            }
        }
        else
        {
            // Local mode or directory, use cached FD.
            lru_fd.Get()->mu_.Unlock();
            return {std::move(lru_fd), KvError::NoError};
        }
    }

    int fd;
    KvError error = KvError::NoError;
    if (file_id == LruFD::kDirectory)
    {
        FdIdx root_fd = GetRootFD(tbl_id);
        std::string dirname = tbl_id.ToString();
        fd = OpenAt(root_fd, dirname.c_str(), oflags_dir, 0, false);
        if (fd == -ENOENT && create)
        {
            fd = MakeDir(root_fd, dirname.c_str());
        }
    }
    else
    {
        uint64_t flags =
            O_RDWR | (direct ? O_DIRECT : 0) | (create ? O_CREAT : 0);
        uint64_t mode = create ? 0644 : 0;
        fd = OpenFile(
            tbl_id, file_id, flags, mode, branch_name, term, skip_cloud_lookup);
        if (fd == -ENOENT && create)
        {
            // This must be data file because manifest should always be
            // created by call WriteSnapshot.
            assert(file_id <= LruFD::kMaxDataFile);
            auto [dfd_ref, err] =
                OpenOrCreateFD(tbl_id, LruFD::kDirectory, false, true, "", 0);
            error = err;
            if (dfd_ref != nullptr)
            {
                TEST_KILL_POINT_WEIGHT("OpenOrCreateFD:CreateFile", 100)
                fd = CreateFile(std::move(dfd_ref), file_id, branch_name, term);
            }
        }
    }

    if (fd < 0)
    {
        // Open or create failed.
        if (error == KvError::NoError)
        {
            error = ToKvError(fd);
        }
        if (file_id == LruFD::kManifest &&
            (error == KvError::NotFound || error == KvError::ResourceMissing))
        {
            // Manifest not found, this is normal so don't log it.
        }
        else
        {
            LOG(ERROR) << "open failed " << tbl_id << " file id " << file_id
                       << " : " << ErrorString(error);
        }
        lru_fd.Get()->mu_.Unlock();
        return {nullptr, error};
    }

    if (file_id == LruFD::kDirectory)
    {
        lru_fd.Get()->fd_ = fd;
        lru_fd.Get()->reg_idx_ = -1;
    }
    else
    {
        lru_fd.Get()->reg_idx_ = fd;
        lru_fd.Get()->fd_ = LruFD::FdEmpty;
    }

    // Set term and branch_name on newly opened file FD (data and manifest).
    if (file_id != LruFD::kDirectory)
    {
        lru_fd.Get()->term_ = term;
        lru_fd.Get()->branch_name_ = InternBranchName(branch_name);
    }
    lru_fd.Get()->mu_.Unlock();
    return {std::move(lru_fd), KvError::NoError};
}

bool IouringMgr::GetBranchNameAndTerm(const TableIdent &tbl_id,
                                      FileId file_id,
                                      std::string &branch_name,
                                      uint64_t &term)
{
    auto it_term_tbl = branch_file_mapping_.find(tbl_id);
    if (it_term_tbl == branch_file_mapping_.end())
    {
        return false;
    }
    const auto &mapping = it_term_tbl->second;
    return ::eloqstore::GetBranchNameAndTerm(
        mapping, file_id, branch_name, term);
}

void IouringMgr::SetBranchFileIdTerm(const TableIdent &tbl_id,
                                     FileId file_id,
                                     std::string_view branch_name,
                                     uint64_t term)
{
    auto &mapping = branch_file_mapping_[tbl_id];

    if (!mapping.empty() && mapping.back().branch_name == branch_name &&
        mapping.back().term == term)
    {
        CHECK(file_id >= mapping.back().max_file_id)
            << "file_id must be allocated in ascending order for the same "
               "branch and term";
        mapping.back().max_file_id = file_id;
    }
    else
    {
        mapping.push_back({std::string(branch_name), term, file_id});
    }
}

void IouringMgr::SetBranchFileMapping(const TableIdent &tbl_id,
                                      BranchFileMapping mapping)
{
    branch_file_mapping_[tbl_id] = std::move(mapping);
}

std::string_view IouringMgr::InternBranchName(std::string_view name)
{
    auto it = branch_name_pool_.find(name);
    if (it != branch_name_pool_.end())
    {
        return *it;
    }
    auto [inserted_it, inserted] = branch_name_pool_.emplace(name);
    return *inserted_it;
}

const BranchFileMapping &IouringMgr::GetBranchFileMapping(
    const TableIdent &tbl_id)
{
    static const BranchFileMapping empty{};
    auto it = branch_file_mapping_.find(tbl_id);
    if (it == branch_file_mapping_.end())
    {
        return empty;
    }
    return it->second;
}

inline uint16_t IouringMgr::LookupRegisteredBufferIndex(const char *ptr) const
{
    DCHECK(buffers_registered_);
    DCHECK(ptr != nullptr);
    DCHECK(registered_buf_base_ != nullptr);
    DCHECK_GT(registered_buf_stride_, 0);
    DCHECK_GT(registered_buf_shift_, 0);
    DCHECK_GT(registered_buf_count_, 0);
    size_t diff = static_cast<size_t>(ptr - registered_buf_base_);
    [[maybe_unused]] size_t max_bytes =
        registered_buf_stride_ * (registered_buf_count_ - 1) +
        registered_last_slice_size_;
    DCHECK_LT(diff, max_bytes);
    size_t idx = diff >> registered_buf_shift_;
    if (idx >= registered_buf_count_)
    {
        idx = registered_buf_count_ - 1;
    }
    return static_cast<uint16_t>(idx);
}

std::pair<FileId, uint32_t> IouringMgr::ConvFilePageId(
    FilePageId file_page_id) const
{
    FileId file_id = file_page_id >> options_->pages_per_file_shift;
    uint32_t offset =
        (file_page_id & ((1 << options_->pages_per_file_shift) - 1)) *
        options_->data_page_size;
    assert(!(offset & (page_align - 1)));
    return {file_id, offset};
}

void IouringMgr::Submit()
{
    const uint32_t prepared_before = prepared_sqe_;
    const uint32_t sq_flags = ring_.sq.kflags == nullptr ? 0 : *ring_.sq.kflags;
    const bool need_taskrun = (sq_flags & IORING_SQ_TASKRUN) != 0;

    if (prepared_before == 0 && !need_taskrun)
    {
        return;
    }

    int ret = 0;
    if (prepared_before == 0)
    {
        // No new SQE, but taskrun is pending. Enter kernel to drive task_work.
        ret = io_uring_enter(
            ring_.ring_fd, 0, 0, IORING_ENTER_GETEVENTS, nullptr);
    }
    else
    {
        ret = io_uring_submit(&ring_);
    }

    if (__builtin_expect(ret < 0, 0))
    {
        if (prepared_before == 0)
        {
            LOG(ERROR) << "io_uring_enter(GETEVENTS) failed " << ret;
        }
        else
        {
            LOG(ERROR) << "iouring submit failed " << ret;
        }
    }
    else if (prepared_before != 0)
    {
        prepared_sqe_ -= ret;
    }
}

void IouringMgr::PollComplete()
{
    io_uring_cqe *cqe = nullptr;
    io_uring_peek_cqe(&ring_, &cqe);
    unsigned head;
    unsigned cnt = 0;
    io_uring_for_each_cqe(&ring_, head, cqe)
    {
        cnt++;

        auto [ptr, type] = DecodeUserData(cqe->user_data);
        KvTask *task = nullptr;
        switch (type)
        {
        case UserDataType::KvTask:
            task = static_cast<KvTask *>(ptr);
            task->io_res_ = cqe->res;
            task->io_flags_ = cqe->flags;
            break;
        case UserDataType::BaseReq:
        {
            BaseReq *req = static_cast<BaseReq *>(ptr);
            req->res_ = cqe->res;
            req->flags_ = cqe->flags;
            task = req->task_;
            break;
        }
        case UserDataType::WriteReq:
        {
            WriteReq *req = static_cast<WriteReq *>(ptr);
            KvError err;
            assert(cqe->res <= options_->data_page_size);
            if (cqe->res < 0)
            {
                err = ToKvError(cqe->res);
            }
            else if (cqe->res < options_->data_page_size)
            {
                err = KvError::TryAgain;
            }
            else
            {
                err = KvError::NoError;
            }
            req->task_->WritePageCallback(std::move(req->page_), err);
            task = req->task_;
            write_req_pool_->Free(req);
            break;
        }
        case UserDataType::MergedWriteReq:
        {
            MergedWriteReq *req = static_cast<MergedWriteReq *>(ptr);
            KvError err;
            if (cqe->res < 0)
            {
                err = ToKvError(cqe->res);
            }
            else if (static_cast<size_t>(cqe->res) < req->bytes_)
            {
                err = KvError::TryAgain;
            }
            else
            {
                err = KvError::NoError;
            }

            for (VarPage &page : req->pages_)
            {
                req->task_->WritePageCallback(std::move(page), err);
                req->task_->FinishIo();
            }
            ReleaseWriteBuffer(req->buf_ptr_, req->buf_index_);
            for (size_t i = 0; i < req->release_ptrs_.size(); ++i)
            {
                if (req->release_ptrs_[i] != nullptr)
                {
                    ReleaseWriteBuffer(req->release_ptrs_[i],
                                       req->release_indices_[i]);
                }
            }
            merged_write_req_pool_->Free(req);
            continue;
        }
        default:
            assert(false);
            continue;
        }
        assert(task != nullptr);
        task->FinishIo();
    }

    io_uring_cq_advance(&ring_, cnt);
    waiting_sqe_.WakeN(cnt);
}

int IouringMgr::MakeDir(FdIdx dir_fd, const char *path)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_mkdirat(sqe, dir_fd.first, path, 0775);
    int res = ThdTask()->WaitIoResult();
    if (res < 0)
    {
        LOG(ERROR) << "mkdirat " << path << " failed " << strerror(-res);
        return res;
    }
    res = Fdatasync(dir_fd);
    if (res < 0)
    {
        LOG(ERROR) << "fsync directory failed " << strerror(-res);
        return res;
    }
    return OpenAt(dir_fd, path, oflags_dir, 0, false);
}

int IouringMgr::CreateFile(LruFD::Ref dir_fd,
                           FileId file_id,
                           std::string_view branch_name,
                           uint64_t term)
{
    assert(file_id <= LruFD::kMaxDataFile);
    uint64_t flags = O_CREAT | O_RDWR | O_DIRECT;
    std::string filename = BranchDataFileName(file_id, branch_name, term);
    int fd = OpenAt(dir_fd.FdPair(), filename.c_str(), flags, 0644);
    if (fd >= 0)
    {
        // Multiple data files may be created in one
        // WriteTask. Table partition directory FD will
        // be marked as dirty every time, but only fsync
        // it once when SyncData.
        dir_fd.Get()->dirty_ = true;
        if (options_->data_append_mode)
        {
            // Avoid update metadata (file size) of file frequently in append
            // write mode.
            Fallocate({fd, true}, options_->DataFileSize());
        }
    }
    return fd;
}

int IouringMgr::OpenFile(const TableIdent &tbl_id,
                         FileId file_id,
                         uint64_t flags,
                         uint64_t mode,
                         std::string_view branch_name,
                         uint64_t term,
                         bool skip_cloud_lookup)
{
    (void) skip_cloud_lookup;
    fs::path path = tbl_id.ToString();
    if (file_id == LruFD::kManifest)
    {
        path.append(BranchManifestFileName(branch_name, term));
    }
    else
    {
        // Data file is always opened with O_DIRECT.
        assert((flags & O_DIRECT) == O_DIRECT);
        assert(file_id <= LruFD::kMaxDataFile);
        std::string filename = BranchDataFileName(file_id, branch_name, term);
        path.append(filename);
    }
    FdIdx root_fd = GetRootFD(tbl_id);
    return OpenAt(root_fd, path.c_str(), flags, mode);
}

int IouringMgr::OpenAt(FdIdx dir_fd,
                       const char *path,
                       uint64_t flags,
                       uint64_t mode,
                       bool fixed_target)
{
    EvictFD();
    uint64_t open_flags = flags;
    if ((open_flags & O_DIRECTORY) == 0)
    {
        open_flags |= O_NOATIME;
    }
    open_how how = {.flags = open_flags, .mode = mode, .resolve = 0};
    if (fixed_target)
    {
        uint32_t idx = AllocRegisterIndex();
        if (idx == UINT32_MAX)
        {
            LOG(ERROR) << "register file slot used up";
            return -EMFILE;
        }
        io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
        io_uring_prep_openat2_direct(sqe, dir_fd.first, path, &how, idx);
        int res = ThdTask()->WaitIoResult();
        if (res < 0)
        {
            FreeRegisterIndex(idx);
            return res;
        }
        lru_fd_count_++;
        return static_cast<int>(idx);
    }

    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_openat2(sqe, dir_fd.first, path, &how);
    int fd = ThdTask()->WaitIoResult();
    if (fd < 0)
    {
        return fd;
    }
    lru_fd_count_++;
    return fd;
}

int IouringMgr::Read(FdIdx fd, char *dst, size_t n, uint64_t offset)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_read(sqe, fd.first, dst, n, offset);
    return ThdTask()->WaitIoResult();
}

int IouringMgr::Write(FdIdx fd, const char *src, size_t n, uint64_t offset)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_write(sqe, fd.first, src, n, offset);
    return ThdTask()->WaitIoResult();
}

KvError IouringMgr::SyncFile(LruFD::Ref fd)
{
    int res = Fdatasync(fd.FdPair());
    if (res == 0)
    {
        fd.Get()->dirty_ = false;
    }
    return ToKvError(res);
}

KvError IouringMgr::FdatasyncFiles(const TableIdent &tbl_id,
                                   std::span<LruFD::Ref> fds)
{
    struct FsyncReq : BaseReq
    {
        FsyncReq(KvTask *task, LruFD::Ref fd)
            : BaseReq(task), fd_ref_(std::move(fd)) {};
        LruFD::Ref fd_ref_;
    };

    // Fsync all dirty files/directory.
    std::vector<FsyncReq> reqs;
    reqs.reserve(fds.size());
    for (LruFD::Ref &fd_ref : fds)
    {
        // FsyncReq elements have pointer stability, because we have reserved
        // enough space for this vector so that it will never reallocate.
        const FsyncReq &req = reqs.emplace_back(ThdTask(), fd_ref);
        auto [fd, registered] = req.fd_ref_.FdPair();
        io_uring_sqe *sqe = GetSQE(UserDataType::BaseReq, &req);
        if (registered)
        {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
    }
    ThdTask()->WaitIo();

    // Check results.
    KvError err = KvError::NoError;
    for (const FsyncReq &req : reqs)
    {
        if (req.res_ < 0)
        {
            err = ToKvError(req.res_);
            LOG(ERROR) << "fsync file failed " << tbl_id << '@'
                       << req.fd_ref_.Get()->file_id_ << " : "
                       << strerror(-req.res_);
        }
    }
    return err;
}

KvError IouringMgr::SyncFiles(const TableIdent &tbl_id,
                              std::span<LruFD::Ref> fds)
{
    KvError err = FdatasyncFiles(tbl_id, fds);
    CHECK_KV_ERR(err);
    for (LruFD::Ref &fd_ref : fds)
    {
        if (auto *fd = fd_ref.Get())
        {
            fd->dirty_ = false;
        }
    }
    return KvError::NoError;
}

KvError IouringMgr::CloseFiles(std::span<LruFD::Ref> fds)
{
    struct CloseReq : BaseReq
    {
        CloseReq(KvTask *task, LruFD::Ref fd)
            : BaseReq(task), fd_ref_(std::move(fd)) {};
        LruFD::Ref fd_ref_;
        int reg_idx_{-1};
        int fd_{LruFD::FdEmpty};
    };

    struct PendingClose
    {
        LruFD::Ref *fd_ref;
        LruFD *lru_fd;
        bool locked;
        int reg_idx;
    };
    // We need to flush dirty pages and close.
    std::vector<PendingClose> pendings;
    pendings.reserve(fds.size());
    std::unordered_map<const TableIdent *, std::vector<LruFD::Ref>>
        dirty_groups;
    dirty_groups.reserve(4);

    for (auto &fd_ref : fds)
    {
        LruFD *lru_fd = fd_ref.Get();
        if (lru_fd == nullptr)
        {
            continue;
        }

        lru_fd->mu_.Lock();
        if (lru_fd->reg_idx_ < 0 && lru_fd->fd_ == LruFD::FdEmpty)
        {
            lru_fd->mu_.Unlock();
            continue;
        }

        pendings.push_back(
            PendingClose{&fd_ref, lru_fd, true, lru_fd->reg_idx_});
        if (lru_fd->dirty_)
        {
            const TableIdent *tbl_id = lru_fd->tbl_->tbl_id_;
            auto [it, _] =
                dirty_groups.try_emplace(tbl_id, std::vector<LruFD::Ref>{});
            it->second.emplace_back(fd_ref);
        }
    }

    auto unlock_pendings = [&pendings]()
    {
        for (size_t i = 0; i < pendings.size(); ++i)
        {
            if (pendings[i].locked)
            {
                pendings[i].lru_fd->mu_.Unlock();
                pendings[i].locked = false;
            }
        }
    };

    KvError err = KvError::NoError;
    for (auto &[tbl_id, refs] : dirty_groups)
    {
        err =
            SyncFiles(*tbl_id, std::span<LruFD::Ref>(refs.data(), refs.size()));
        if (err != KvError::NoError)
        {
            unlock_pendings();
            return err;
        }
    }

    std::vector<CloseReq> reqs;
    reqs.reserve(pendings.size());

    for (size_t idx = 0; idx < pendings.size(); ++idx)
    {
        PendingClose &pending = pendings[idx];
        if (!pending.locked)
        {
            continue;
        }
        LruFD *lru_fd = pending.lru_fd;
        LruFD::Ref &fd_ref = *pending.fd_ref;

        CloseReq &req = reqs.emplace_back(ThdTask(), std::move(fd_ref));
        io_uring_sqe *sqe = GetSQE(UserDataType::BaseReq, &req);
        if (lru_fd->reg_idx_ < 0)
        {
            req.fd_ = lru_fd->fd_;
            lru_fd->fd_ = LruFD::FdEmpty;
            io_uring_prep_close(sqe, req.fd_);
        }
        else
        {
            req.reg_idx_ = pending.reg_idx;
            lru_fd->reg_idx_ = -1;
            lru_fd->fd_ = LruFD::FdEmpty;
            io_uring_prep_close_direct(sqe, req.reg_idx_);
        }

        lru_fd->mu_.Unlock();
        pending.locked = false;
    }

    KvError close_err = KvError::NoError;
    if (!reqs.empty())
    {
        ThdTask()->WaitIo();

        for (const CloseReq &req : reqs)
        {
            if (req.res_ < 0)
            {
                LOG(ERROR) << "close file failed file_id="
                           << req.fd_ref_.Get()->file_id_ << " : "
                           << strerror(-req.res_);
                if (close_err == KvError::NoError)
                {
                    close_err = ToKvError(req.res_);
                }
            }
            if (req.reg_idx_ >= 0)
            {
                FreeRegisterIndex(req.reg_idx_);
            }
            if (req.res_ == 0)
            {
                lru_fd_count_--;
            }
        }
    }
    DLOG(INFO) << "Close files";
    return close_err;
}

int IouringMgr::Fdatasync(FdIdx fd)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_fsync(sqe, fd.first, IORING_FSYNC_DATASYNC);
    return ThdTask()->WaitIoResult();
}

int IouringMgr::Statx(FdIdx fd, const char *path, struct statx *result)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    CHECK(!fd.second) << "statx does not support fixed file";
    io_uring_prep_statx(
        sqe, fd.first, path, AT_EMPTY_PATH, STATX_BASIC_STATS, result);
    return ThdTask()->WaitIoResult();
}

int IouringMgr::StatxAt(FdIdx dir_fd, const char *path, struct statx *result)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_statx(sqe, dir_fd.first, path, 0, STATX_BASIC_STATS, result);
    return ThdTask()->WaitIoResult();
}

int IouringMgr::Rename(FdIdx dir_fd, const char *old_path, const char *new_path)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_renameat(
        sqe, dir_fd.first, old_path, dir_fd.first, new_path, 0);
    return ThdTask()->WaitIoResult();
}

int IouringMgr::Close(int fd)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_close(sqe, fd);
    int res = ThdTask()->WaitIoResult();
    if (res < 0)
    {
        LOG(ERROR) << "close file/directory " << fd
                   << " failed: " << strerror(-res);
    }
    else
    {
        lru_fd_count_--;
    }
    return res;
}

int IouringMgr::CloseDirect(int idx)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_close_direct(sqe, idx);
    int res = ThdTask()->WaitIoResult();
    if (res < 0)
    {
        LOG(ERROR) << "close direct file " << idx
                   << " failed: " << strerror(-res);
    }
    if (res == 0)
    {
        FreeRegisterIndex(idx);
        lru_fd_count_--;
    }
    return res;
}

KvError IouringMgr::CloseFile(LruFD::Ref fd_ref)
{
    LruFD *lru_fd = fd_ref.Get();
    const int fd_idx = lru_fd->reg_idx_;
    if (fd_idx < 0)
    {
        const int fd = lru_fd->fd_;
        if (fd < 0)
        {
            return KvError::NoError;
        }
        // Make sure no tasks can use this fd during closing.
        lru_fd->mu_.Lock();
        if (lru_fd->dirty_)
        {
            if (KvError err = SyncFile(fd_ref); err != KvError::NoError)
            {
                lru_fd->mu_.Unlock();
                return err;
            }
        }
        lru_fd->fd_ = LruFD::FdEmpty;
        int res = Close(fd);
        lru_fd->mu_.Unlock();
        return ToKvError(res);
    }

    // Make sure no tasks can use this fd during closing.
    lru_fd->mu_.Lock();

    if (lru_fd->dirty_)
    {
        if (KvError err = SyncFile(fd_ref); err != KvError::NoError)
        {
            lru_fd->mu_.Unlock();
            return err;
        }
    }

    lru_fd->reg_idx_ = -1;
    lru_fd->fd_ = LruFD::FdEmpty;
    int res = CloseDirect(fd_idx);
    lru_fd->mu_.Unlock();
    return ToKvError(res);
}

bool IouringMgr::EvictFD()
{
    while (lru_fd_count_ > fd_limit_ ||
           (free_reg_slots_.empty() && alloc_reg_slot_ >= fd_limit_))
    {
        LruFD *lru_fd = lru_fd_tail_.prev_;
        if (lru_fd == &lru_fd_head_)
        {
            return false;
        }
        assert(lru_fd->ref_count_ == 0);
        assert(lru_fd->reg_idx_ >= 0 || lru_fd->fd_ >= 0);
        // This LruFD will be removed by ~LruFD::Ref if succeed to close,
        // otherwise be enqueued back to LRU.
        CloseFile(LruFD::Ref(lru_fd, this));
    }
    return true;
}

uint32_t IouringMgr::AllocRegisterIndex()
{
    uint32_t idx = UINT32_MAX;
    if (free_reg_slots_.empty())
    {
        if (alloc_reg_slot_ < fd_limit_)
        {
            idx = alloc_reg_slot_++;
        }
    }
    else
    {
        idx = free_reg_slots_.back();
        free_reg_slots_.pop_back();
    }
    return idx;
}

void IouringMgr::FreeRegisterIndex(uint32_t idx)
{
    free_reg_slots_.push_back(idx);
}

bool IouringMgr::HasOtherFile(const TableIdent &tbl_id) const
{
    fs::path dir_path =
        tbl_id.StorePath(options_->store_path, options_->store_path_lut);
    std::error_code ec;
    if (!fs::exists(dir_path, ec) || !fs::is_directory(dir_path, ec))
    {
        return false;
    }

    fs::directory_iterator it(dir_path, ec);
    if (ec)
    {
        LOG(WARNING) << "Failed to iterate partition directory " << dir_path
                     << " for table " << tbl_id << ": " << ec.message();
        return false;
    }

    for (; it != fs::directory_iterator(); ++it)
    {
        const std::string name = it->path().filename().string();
        if (boost::algorithm::ends_with(name, TmpSuffix))
        {
            continue;
        }
        // Ignore current manifest files (manifest or manifest_<term>), but do
        // not ignore archive manifests (manifest_<term>_<tag>).
        auto [type, suffix] = ParseFileName(name);
        if (type == FileNameManifest)
        {
            std::string_view branch_name;
            uint64_t term = 0;
            std::optional<std::string> tag;
            if (ParseManifestFileSuffix(suffix, branch_name, term, tag) &&
                !tag.has_value())
            {
                continue;
            }
        }
        return true;
    }

    return false;
}

int IouringMgr::Fallocate(FdIdx fd, uint64_t size)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_fallocate(sqe, fd.first, 0, 0, size);
    int res = ThdTask()->WaitIoResult();
    if (res < 0)
    {
        LOG(ERROR) << "fallocate failed " << strerror(-res);
    }
    return res;
}

int IouringMgr::UnlinkAt(FdIdx dir_fd, const char *path, bool rmdir)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    if (dir_fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    int flags = 0;
    if (rmdir)
    {
        flags = AT_REMOVEDIR;
    }
    io_uring_prep_unlinkat(sqe, dir_fd.first, path, flags);
    int res = ThdTask()->WaitIoResult();
    return res;
}

KvError IouringMgr::AppendManifest(const TableIdent &tbl_id,
                                   std::string_view log,
                                   uint64_t offset)
{
    if (log.empty())
    {
        return KvError::NoError;
    }

    uint64_t manifest_term;
    std::string_view manifest_br;
    manifest_br = GetActiveBranch();
    manifest_term = ProcessTerm();
    std::string_view active_br = manifest_br;
#ifndef NDEBUG
    const PageId root =
        DecodeFixed32(log.data() + ManifestBuilder::offset_root);
    const PageId ttl_root =
        DecodeFixed32(log.data() + ManifestBuilder::offset_ttl_root);
    const uint32_t payload_len =
        DecodeFixed32(log.data() + ManifestBuilder::offset_len);
    const std::string_view record_view{
        log.data(), ManifestBuilder::header_bytes + payload_len};
    const uint64_t checksum = DecodeFixed64(log.data());
    DLOG(INFO) << "AppendManifest tbl=" << tbl_id << " manifest="
               << BranchManifestFileName(manifest_br, manifest_term)
               << " offset=" << offset << " bytes=" << log.size()
               << " payload=" << payload_len << " root=" << root
               << " ttl_root=" << ttl_root << " checksum=" << checksum
               << " record size="
               << ManifestBuilder::header_bytes + payload_len;
    const bool checksum_ok = ManifestBuilder::ValidateChecksum(record_view);
    assert(checksum_ok);
#endif
    // Record manifest write payload for cloud upload before submit
    // (manifest segments are tracked too).
    OnFileRangeWritePrepared(
        tbl_id, LruFD::kManifest, active_br, manifest_term, offset, log);
    auto [fd_ref, err] =
        OpenFD(tbl_id, LruFD::kManifest, true, active_br, manifest_term);
    CHECK_KV_ERR(err);
    fd_ref.Get()->dirty_ = true;

    TEST_KILL_POINT_WEIGHT("AppendManifest:Write", 10)

    [[maybe_unused]] const size_t alignment = page_align;
    assert((offset & (alignment - 1)) == 0);
    assert((log.size() & (alignment - 1)) == 0);

    const size_t write_batch_size = page_align;
    size_t remaining = log.size();
    size_t written = 0;
    FdIdx fd_idx = fd_ref.FdPair();
    while (remaining > 0)
    {
        size_t batch = std::min(write_batch_size, remaining);
        int wres = Write(fd_idx, log.data() + written, batch, offset + written);
        if (wres < 0)
        {
            LOG(ERROR) << "append manifest failed " << tbl_id;
            return ToKvError(wres);
        }
        if (wres == 0)
        {
            LOG(ERROR) << "append manifest wrote zero bytes " << tbl_id;
            return KvError::TryAgain;
        }
        written += static_cast<size_t>(wres);
        if (remaining >= static_cast<size_t>(wres))
        {
            remaining -= static_cast<size_t>(wres);
        }
        else
        {
            remaining = 0;
        }
    }

    TEST_KILL_POINT_WEIGHT("AppendManifest:Sync", 10)
    return SyncFile(std::move(fd_ref));
}

KvError CloudStoreMgr::AppendManifest(const TableIdent &tbl_id,
                                      std::string_view log,
                                      uint64_t offset)
{
    auto [dir_fd, dir_err] =
        OpenOrCreateFD(tbl_id, LruFD::kDirectory, false, true, "", 0);
    if (dir_err != KvError::NoError)
    {
        LOG(ERROR) << "Cloud AppendManifest ensure dir failed for " << tbl_id
                   << ": " << ErrorString(dir_err);
        return dir_err;
    }
    return IouringMgr::AppendManifest(tbl_id, log, offset);
}

int IouringMgr::WriteSnapshot(LruFD::Ref dir_fd,
                              std::string_view name,
                              std::string_view content)
{
    assert(!content.empty());
    std::string tmpfile = std::string(name) + TmpSuffix;
    uint64_t tmp_oflags = O_CREAT | O_TRUNC | O_RDWR | O_DIRECT;
    int tmp_fd = OpenAt(dir_fd.FdPair(), tmpfile.c_str(), tmp_oflags, 0644);
    if (tmp_fd < 0)
    {
        LOG(ERROR) << "create temporary file failed " << strerror(-tmp_fd);
        return tmp_fd;
    }

    const char *write_ptr = content.data();
    size_t io_size = content.size();
    [[maybe_unused]] const size_t alignment = page_align;
    assert((io_size & (alignment - 1)) == 0);
    assert((reinterpret_cast<uintptr_t>(write_ptr) & (alignment - 1)) == 0);
    FdIdx tmp_fd_idx{tmp_fd, true};
    size_t write_batch_size = Options()->non_page_io_batch_size;
    size_t remaining = io_size;
    size_t written = 0;
    while (remaining > 0)
    {
        size_t batch = std::min(write_batch_size, remaining);
        int res = Write(tmp_fd_idx, write_ptr + written, batch, written);
        if (res < 0)
        {
            CloseDirect(tmp_fd);
            LOG(ERROR) << "write snapshot failed: " << strerror(-res);
            return res;
        }
        if (res == 0)
        {
            CloseDirect(tmp_fd);
            LOG(ERROR) << "write temporary file wrote zero bytes.";
            return -EIO;
        }
        written += static_cast<size_t>(res);
        if (remaining >= static_cast<size_t>(res))
        {
            remaining -= static_cast<size_t>(res);
        }
        else
        {
            remaining = 0;
        }
    }

    TEST_KILL_POINT("AtomicWriteFile:Sync")
    int res = Fdatasync({tmp_fd, true});
    if (res < 0)
    {
        CloseDirect(tmp_fd);
        LOG(ERROR) << "fsync temporary file failed " << strerror(-res);
        return res;
    }

    // Switch file on disk.
    TEST_KILL_POINT("AtomicWriteFile:Rename")
    res = Rename(dir_fd.FdPair(), tmpfile.c_str(), name.data());
    if (res < 0)
    {
        CloseDirect(tmp_fd);
        LOG(ERROR) << "rename temporary file failed " << strerror(-res);
        return res;
    }
    TEST_KILL_POINT("AtomicWriteFile:SyncDir")
    res = Fdatasync(dir_fd.FdPair());
    if (res < 0)
    {
        CloseDirect(tmp_fd);
        LOG(ERROR) << "fsync directory failed " << strerror(-res);
        return res;
    }

    return tmp_fd;
}

KvError IouringMgr::SwitchManifest(const TableIdent &tbl_id,
                                   std::string_view snapshot)
{
    LruFD::Ref fd_ref = GetOpenedFD(tbl_id, LruFD::kManifest);
    if (fd_ref != nullptr)
    {
        // Close the old manifest firstly.
        KvError err = CloseFile(std::move(fd_ref));
        CHECK_KV_ERR(err);
    }

    uint64_t manifest_term = ProcessTerm();
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    CHECK_KV_ERR(err);
    std::string_view active_br = GetActiveBranch();
    const std::string manifest_name =
        BranchManifestFileName(active_br, manifest_term);
    int res = WriteSnapshot(std::move(dir_fd), manifest_name, snapshot);
    if (res < 0)
    {
        LOG(ERROR) << "SwitchManifest WriteSnapshot failed for " << tbl_id
                   << " manifest=" << manifest_name << " : " << strerror(-res);
        return ToKvError(res);
    }
    CloseDirect(res);
    return KvError::NoError;
}

KvError IouringMgr::CreateArchive(const TableIdent &tbl_id,
                                  std::string_view branch_name,
                                  uint64_t term,
                                  std::string_view snapshot,
                                  std::string_view tag)
{
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    CHECK_KV_ERR(err);
    const std::string name = BranchArchiveName(branch_name, term, tag);
    int res = WriteSnapshot(std::move(dir_fd), name, snapshot);
    if (res < 0)
    {
        return ToKvError(res);
    }
    CloseDirect(res);
    return KvError::NoError;
}

KvError IouringMgr::DeleteArchive(const TableIdent &tbl_id,
                                  std::string_view branch_name,
                                  uint64_t term,
                                  std::string_view tag)
{
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    CHECK_KV_ERR(err);
    if (term == std::numeric_limits<uint64_t>::max())
    {
        term = ProcessTerm();
    }
    const std::string name = BranchArchiveName(branch_name, term, tag);
    const int res = UnlinkAt(dir_fd.FdPair(), name.c_str(), false);
    if (res < 0 && res != -ENOENT)
    {
        return ToKvError(res);
    }
    return KvError::NoError;
}

KvError IouringMgr::WriteBranchManifest(const TableIdent &tbl_id,
                                        std::string_view branch_name,
                                        uint64_t term,
                                        std::string_view snapshot)
{
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    CHECK_KV_ERR(err);

    // Generate branch manifest filename: manifest_<branch_name>_<term>
    const std::string name = BranchManifestFileName(branch_name, term);

    int res = WriteSnapshot(std::move(dir_fd), name, snapshot);
    if (res < 0)
    {
        return ToKvError(res);
    }
    CloseDirect(res);
    return KvError::NoError;
}

KvError IouringMgr::DeleteBranchFiles(
    const TableIdent &tbl_id,
    std::string_view branch_name,
    uint64_t /* term (unused: we read from CURRENT_TERM) */)
{
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    CHECK_KV_ERR(err);

    // Store root fd for reading/deleting CURRENT_TERM_<branch>_<pg_id>
    assert(!eloq_store->root_fds_.empty());
    int root_fd = eloq_store->root_fds_[0];
    FdIdx root_fd_idx{root_fd, false};

    std::string current_term_filename =
        CurrentTermFileNameForBranchAndPartitionGroup(
            branch_name, eloq_store->PartitionGroupId());

    // Read CURRENT_TERM_<branch>_<pg_id> from store root to find the highest
    // term written for this branch. If the file is missing, fall back to term
    // 0 so we still delete manifest_<branch>_0.
    uint64_t max_term = 0;
    {
        int ct_fd =
            OpenAt(root_fd_idx, current_term_filename.c_str(), O_RDONLY, 0);
        if (ct_fd >= 0)
        {
            char buf[32] = {};
            // OpenAt defaults to fixed_target=true; ct_fd is a registered
            // index.
            int n = Read(FdIdx{ct_fd, true}, buf, sizeof(buf) - 1, 0);
            if (n > 0 && !ParseBranchTerm(std::string_view(buf, n), max_term))
            {
                CloseDirect(ct_fd);
                return KvError::IoFail;
            }
            CloseDirect(ct_fd);
        }
    }

    // Build the delete list for manifests (in table directory).
    // ENOENT is ignored per file so gaps are safe.
    std::vector<std::string> table_files_to_delete;
    table_files_to_delete.reserve(max_term + 1);
    for (uint64_t t = 0; t <= max_term; ++t)
    {
        table_files_to_delete.push_back(BranchManifestFileName(branch_name, t));
    }

    // Use the directory fd for unlink operations on table files
    KvTask *current_task = ThdTask();
    struct UnlinkReq : BaseReq
    {
        std::string path;
    };
    std::vector<UnlinkReq> reqs;
    reqs.reserve(table_files_to_delete.size() + 1);  // +1 for term file

    auto dir_fd_pair = dir_fd.FdPair();
    for (const std::string &file_path : table_files_to_delete)
    {
        reqs.emplace_back();
        reqs.back().task_ = current_task;
        reqs.back().path = file_path;
        io_uring_sqe *unlink_sqe = GetSQE(UserDataType::BaseReq, &reqs.back());
        if (dir_fd_pair.second)
        {
            unlink_sqe->flags |= IOSQE_FIXED_FILE;
        }
        io_uring_prep_unlinkat(
            unlink_sqe, dir_fd_pair.first, file_path.c_str(), 0);
    }

    // Also unlink the CURRENT_TERM file from store root
    {
        reqs.emplace_back();
        reqs.back().task_ = current_task;
        reqs.back().path = current_term_filename;
        io_uring_sqe *unlink_sqe = GetSQE(UserDataType::BaseReq, &reqs.back());
        // root_fd is not a registered fd, so don't set IOSQE_FIXED_FILE
        io_uring_prep_unlinkat(
            unlink_sqe, root_fd, current_term_filename.c_str(), 0);
    }

    current_task->WaitIo();

    KvError first_error = KvError::NoError;
    for (const auto &req : reqs)
    {
        // Ignore ENOENT: not every term slot is guaranteed to have a manifest.
        if (req.res_ < 0 && req.res_ != -ENOENT &&
            first_error == KvError::NoError)
        {
            LOG(ERROR) << "Failed to unlink file: " << req.path
                       << ", error: " << req.res_;
            first_error = ToKvError(req.res_);
        }
    }

    return first_error;
}

io_uring_sqe *IouringMgr::GetSQE(UserDataType type, const void *user_ptr)
{
    io_uring_sqe *sqe;
    while ((sqe = io_uring_get_sqe(&ring_)) == NULL)
    {
        waiting_sqe_.Wait(ThdTask());
    }

    if (user_ptr != nullptr)
    {
        EncodeUserData(sqe, user_ptr, type);
    }
    // SQE objects are reused by io_uring. Clear per-request flags so fixed-fd
    // state does not leak to non-fixed operations.
    sqe->flags = 0;
    ThdTask()->inflight_io_++;
    prepared_sqe_++;
    return sqe;
}

IouringMgr::WriteReqPool::WriteReqPool(uint32_t pool_size)
{
    assert(pool_size > 0);
    pool_ = std::make_unique<WriteReq[]>(pool_size);
    free_list_ = nullptr;
    for (size_t i = 0; i < pool_size; i++)
    {
        Free(&pool_[i]);
    }
}

IouringMgr::WriteReq *IouringMgr::WriteReqPool::Alloc(LruFD::Ref fd,
                                                      VarPage page)
{
    while (free_list_ == nullptr)
    {
        waiting_.Wait(ThdTask());
    }
    WriteReq *req = free_list_;
    free_list_ = req->next_;

    req->next_ = nullptr;
    req->fd_ref_ = std::move(fd);
    req->SetPage(std::move(page));
    req->task_ = static_cast<WriteTask *>(ThdTask());
    return req;
}

void IouringMgr::WriteReqPool::Free(WriteReq *req)
{
    req->fd_ref_ = nullptr;

    req->next_ = free_list_;
    free_list_ = req;
    waiting_.WakeOne();
}

IouringMgr::MergedWriteReqPool::MergedWriteReqPool(uint32_t pool_size)
{
    assert(pool_size > 0);
    pool_ = std::make_unique<MergedWriteReq[]>(pool_size);
    free_list_ = nullptr;
    for (size_t i = 0; i < pool_size; i++)
    {
        pool_[i].pages_.clear();
        pool_[i].task_ = nullptr;
        pool_[i].next_ = free_list_;
        free_list_ = &pool_[i];
    }
}

IouringMgr::MergedWriteReq *IouringMgr::MergedWriteReqPool::Alloc(
    WriteTask *task,
    LruFD::Ref fd,
    char *buf_ptr,
    uint16_t buf_index,
    size_t bytes,
    uint64_t offset,
    std::vector<VarPage> pages)
{
    while (free_list_ == nullptr)
    {
        waiting_.Wait(ThdTask());
    }
    MergedWriteReq *req = free_list_;
    free_list_ = req->next_;

    req->task_ = task;
    req->fd_ref_ = std::move(fd);
    req->buf_ptr_ = buf_ptr;
    req->buf_index_ = buf_index;
    req->bytes_ = bytes;
    req->offset_ = offset;
    req->pages_ = std::move(pages);
    req->next_ = nullptr;
    return req;
}

void IouringMgr::MergedWriteReqPool::Free(MergedWriteReq *req)
{
    req->pages_.clear();
    req->release_ptrs_.clear();
    req->release_indices_.clear();
    req->fd_ref_ = nullptr;
    req->task_ = nullptr;
    req->buf_ptr_ = nullptr;
    req->buf_index_ = 0;
    req->use_fixed_ = true;
    req->bytes_ = 0;
    req->offset_ = 0;
    req->next_ = free_list_;
    free_list_ = req;
    waiting_.WakeOne();
}

IouringMgr::Manifest::Manifest(IouringMgr *io_mgr, LruFD::Ref fd, uint64_t size)
    : io_mgr_(io_mgr), fd_(std::move(fd)), file_size_(size)
{
    char *p = (char *) std::aligned_alloc(page_align, buf_size);
    assert(p);
#ifndef NDEBUG
    // Fill with junk data for debugging purposes.
    std::memset(p, 123, buf_size);
#endif
    buf_ = {p, std::free};
};

IouringMgr::Manifest::~Manifest()
{
    KvError err = io_mgr_->CloseFile(std::move(fd_));
    if (err != KvError::NoError)
    {
        LOG(ERROR) << "close direct manifest file failed: " << ErrorString(err);
    }
}

KvError IouringMgr::Manifest::EnsureBuffered()
{
    while (buf_offset_ >= buf_end_)
    {
        if (file_offset_ >= file_size_)
        {
            return KvError::EndOfFile;
        }
        size_t expected = std::min(file_size_ - file_offset_, size_t(buf_size));
        int res =
            io_mgr_->Read(fd_.FdPair(), buf_.get(), buf_size, file_offset_);
        if (res < 0)
        {
            KvError err = ToKvError(res);
            if (err == KvError::TryAgain)
            {
                continue;
            }
            return err;
        }
        else if (res == 0)
        {
            return KvError::EndOfFile;
        }
        else if (static_cast<size_t>(res) < expected)
        {
            // Read less than expected, ensure next read starts from alignment.
            res &= ~(page_align - 1);
        }

        file_offset_ += res;
        buf_end_ = res;
        buf_offset_ = 0;
    }
    return KvError::NoError;
}

KvError IouringMgr::Manifest::Read(char *dst, size_t n)
{
    while (n > 0)
    {
        KvError err = EnsureBuffered();
        if (err != KvError::NoError)
        {
            return err;
        }

        size_t bytes = std::min(size_t(buf_end_ - buf_offset_), n);
        memcpy(dst, buf_.get() + buf_offset_, bytes);
        buf_offset_ += bytes;
        dst += bytes;
        n -= bytes;
    }
    return KvError::NoError;
}

KvError IouringMgr::Manifest::SkipPadding(size_t n)
{
    while (n > 0)
    {
        KvError err = EnsureBuffered();
        if (err != KvError::NoError)
        {
            return err;
        }
        size_t bytes = std::min(size_t(buf_end_ - buf_offset_), n);
        buf_offset_ += bytes;
        n -= bytes;
    }
    return KvError::NoError;
}

IouringMgr::LruFD::LruFD(PartitionFiles *tbl, FileId file_id, uint64_t term)
    : tbl_(tbl), file_id_(file_id), term_(term)
{
}

std::pair<int, bool> IouringMgr::LruFD::FdPair() const
{
    bool registered = reg_idx_ >= 0;
    return {registered ? reg_idx_ : fd_, registered};
}

void IouringMgr::LruFD::Deque()
{
    LruFD *prev = prev_;
    LruFD *next = next_;

    if (prev != nullptr)
    {
        prev->next_ = next;
    }
    if (next != nullptr)
    {
        next->prev_ = prev;
    }
    prev_ = nullptr;
    next_ = nullptr;
}

void IouringMgr::LruFD::EnqueNext(LruFD *new_fd)
{
    LruFD *old_next = next_;
    next_ = new_fd;
    new_fd->prev_ = this;

    new_fd->next_ = old_next;
    if (old_next != nullptr)
    {
        old_next->prev_ = new_fd;
    }
}

IouringMgr::LruFD::Ref::Ref(LruFD *fd_ptr, IouringMgr *io_mgr)
    : fd_(fd_ptr), io_mgr_(io_mgr)
{
    if (fd_)
    {
        assert(io_mgr_);
        if (fd_->ref_count_++ == 0)
        {
            fd_->Deque();
        }
    }
}

IouringMgr::LruFD::Ref::Ref(Ref &&other) noexcept
    : fd_(other.fd_), io_mgr_(other.io_mgr_)
{
    other.fd_ = nullptr;
    other.io_mgr_ = nullptr;
}

IouringMgr::LruFD::Ref::Ref(const Ref &other)
    : fd_(other.fd_), io_mgr_(other.io_mgr_)
{
    if (fd_)
    {
        assert(io_mgr_);
        assert(fd_->ref_count_ > 0);
        fd_->ref_count_++;
    }
}

IouringMgr::LruFD::Ref &IouringMgr::LruFD::Ref::operator=(Ref &&other) noexcept
{
    if (this != &other)
    {
        if (fd_)
        {
            Clear();
        }
        fd_ = other.fd_;
        io_mgr_ = other.io_mgr_;
        other.fd_ = nullptr;
        other.io_mgr_ = nullptr;
    }
    return *this;
}

IouringMgr::LruFD::Ref::~Ref()
{
    if (fd_)
    {
        Clear();
    }
}

bool IouringMgr::LruFD::Ref::operator==(const Ref &other) const
{
    return fd_ == other.fd_;
}

std::pair<int, bool> IouringMgr::LruFD::Ref::FdPair() const
{
    return fd_->FdPair();
}

IouringMgr::LruFD *IouringMgr::LruFD::Ref::Get() const
{
    return fd_;
}

void IouringMgr::LruFD::Ref::Clear()
{
    if (--fd_->ref_count_ == 0)
    {
        PartitionFiles *partition_files = fd_->tbl_;
        if (fd_->reg_idx_ >= 0 || fd_->fd_ >= 0)
        {
            io_mgr_->lru_fd_head_.EnqueNext(fd_);
        }
        else
        {
            partition_files->fds_.erase(fd_->file_id_);
            if (partition_files->fds_.empty())
            {
                io_mgr_->tables_.erase(*partition_files->tbl_id_);
            }
        }
    }
    fd_ = nullptr;
    io_mgr_ = nullptr;
}

char *IouringMgr::WriteReq::PagePtr() const
{
    return VarPagePtr(page_);
}

void IouringMgr::WriteReq::SetPage(VarPage page)
{
    switch (VarPageType(page.index()))
    {
    case VarPageType::MemIndexPage:
        page_.emplace<MemIndexPage::Handle>(
            std::move(std::get<MemIndexPage::Handle>(page)));
        break;
    case VarPageType::DataPage:
        page_.emplace<DataPage>(std::move(std::get<DataPage>(page)));
        break;
    case VarPageType::OverflowPage:
        page_.emplace<OverflowPage>(std::move(std::get<OverflowPage>(page)));
        break;
    case VarPageType::Page:
        page_.emplace<Page>(std::move(std::get<Page>(page)));
        break;
    }
}
KvError IouringMgr::DeleteFiles(const std::vector<std::string> &file_paths)
{
    if (file_paths.empty())
    {
        return KvError::NoError;
    }

    KvTask *current_task = ThdTask();
    struct UnlinkReq : BaseReq
    {
        std::string path;
    };
    std::vector<UnlinkReq> reqs;
    reqs.reserve(file_paths.size());

    // Submit all unlink operations
    for (const std::string &file_path : file_paths)
    {
        reqs.emplace_back();
        reqs.back().task_ = current_task;
        reqs.back().path = file_path;
        io_uring_sqe *unlink_sqe = GetSQE(UserDataType::BaseReq, &reqs.back());
        io_uring_prep_unlinkat(unlink_sqe, AT_FDCWD, file_path.c_str(), 0);
    }

    current_task->WaitIo();

    KvError first_error = KvError::NoError;
    for (const auto &req : reqs)
    {
        if (req.res_ < 0 && first_error == KvError::NoError)
        {
            LOG(ERROR) << "Failed to unlink file: " << req.path
                       << ", error: " << req.res_;
            first_error = ToKvError(req.res_);
        }
    }

    return first_error;
}

CloudStoreMgr::CloudStoreMgr(const KvOptions *opts,
                             uint32_t fd_limit,
                             CloudStorageService *service)
    : IouringMgr(opts, fd_limit),
      file_cleaner_(this),
      direct_io_buffer_pool_(opts->direct_io_buffer_pool_size, true),
      obj_store_(opts, service),
      cloud_service_(service)
{
    lru_file_head_.next_ = &lru_file_tail_;
    lru_file_tail_.prev_ = &lru_file_head_;
    shard_local_space_limit_ = opts->local_space_limit / opts->num_threads;
    prewarmers_.reserve(opts->prewarm_task_count);
    for (uint16_t i = 0; i < opts->prewarm_task_count; ++i)
    {
        prewarmers_.emplace_back(std::make_unique<Prewarmer>(this));
    }
    DirectIoBuffer::UpdateDefaultReserve(
        std::max(options_->DataFileSize(), static_cast<size_t>(8 * MB)));
}

CloudStoreMgr::~CloudStoreMgr()
{
    if (cloud_service_)
    {
        cloud_service_->UnregisterObjectStore(shard_id_);
    }
}

KvError CloudStoreMgr::Init(Shard *shard)
{
    shard_id_ = shard->shard_id_;  // Store for NotifyWorker calls
    KvError err = IouringMgr::Init(shard);
    CHECK_KV_ERR(err);

    if (cloud_service_)
    {
        cloud_service_->RegisterObjectStore(&obj_store_, shard_id_);
    }

    if (shard_id_ == 0)
    {
        err = obj_store_.EnsureBucketExists();
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

KvError CloudStoreMgr::RestoreStartupState()
{
    if (!options_->allow_reuse_local_caches)
    {
        return KvError::NoError;
    }
    return RestoreLocalCacheState();
}

void CloudStoreMgr::OnFileRangeWritePrepared(const TableIdent &tbl_id,
                                             FileId file_id,
                                             std::string_view branch_name,
                                             uint64_t term,
                                             uint64_t offset,
                                             std::string_view data)
{
    if (data.empty())
    {
        return;
    }
    auto *owner = CurrentWriteTask();
    if (owner == nullptr)
    {
        return;
    }

    const std::string filename =
        (file_id == LruFD::kManifest)
            ? BranchManifestFileName(branch_name, term)
            : BranchDataFileName(file_id, branch_name, term);
    WriteTask::UploadState &state = owner->MutableUploadState();
    if (state.invalid)
    {
        LOG(ERROR) << "WriteTask upload state already invalid, table=" << tbl_id
                   << " filename=" << state.filename << " offset=" << offset
                   << " bytes=" << data.size();
        return;
    }

    if (state.initialized)
    {
        if (state.filename != filename)
        {
            state.invalid = true;
            LOG(ERROR) << "WriteTask upload state switched file before upload, "
                       << "table=" << tbl_id << " previous=" << state.filename
                       << " current=" << filename;
            return;
        }
        if (offset != state.end_offset)
        {
            state.invalid = true;
            LOG(ERROR) << "WriteTask upload buffer must append continuously, "
                       << "table=" << tbl_id << " filename=" << filename
                       << " expected_offset=" << state.end_offset
                       << " actual_offset=" << offset;
            return;
        }
    }
    else
    {
        state.filename = filename;
        state.start_offset = offset;
        state.end_offset = offset;
        state.initialized = true;
    }

    const uint64_t bytes = data.size();
    if (offset > std::numeric_limits<uint64_t>::max() - bytes)
    {
        state.invalid = true;
        LOG(ERROR) << "WriteTask upload state offset overflow, table=" << tbl_id
                   << " filename=" << filename << " offset=" << offset
                   << " bytes=" << bytes;
        return;
    }
    uint64_t new_end = offset + bytes;
    if (new_end > std::numeric_limits<size_t>::max())
    {
        state.invalid = true;
        LOG(ERROR) << "WriteTask upload state size overflow, table=" << tbl_id
                   << " filename=" << filename << " end_offset=" << new_end;
        return;
    }
    owner->EnsureUploadStateBuffer();
    state.buffer.resize(static_cast<size_t>(new_end));
    std::memcpy(state.buffer.data() + static_cast<size_t>(offset),
                data.data(),
                data.size());
    state.end_offset = new_end;
}

KvError CloudStoreMgr::OnDataFileSealed(const TableIdent &tbl_id,
                                        FileId file_id)
{
    assert(file_id <= LruFD::kMaxDataFile);
    LruFD::Ref fd_ref = GetOpenedFD(tbl_id, file_id);
    WriteTask *owner = CurrentWriteTask();
    uint64_t term = ProcessTerm();
    std::string_view branch;
    std::string branch_str;
    if (fd_ref != nullptr)
    {
        term = fd_ref.Get()->term_;
        branch = fd_ref.Get()->branch_name_;
    }
    else
    {
        GetBranchNameAndTerm(tbl_id, file_id, branch_str, term);
        branch = branch_str.empty() ? GetActiveBranch() : branch_str;
    }
    KvError err = UploadFile(tbl_id,
                             BranchDataFileName(file_id, branch, term),
                             owner,
                             {},
                             owner == nullptr,
                             fd_ref.FdPair());
    if (err != KvError::NoError)
    {
        return err;
    }
    if (fd_ref != nullptr)
    {
        // This sealed file already has an upload in flight and should not be
        // re-uploaded again during the final SyncData() pass.
        fd_ref.Get()->dirty_ = false;
    }
    return KvError::NoError;
}

KvError CloudStoreMgr::ReadFilePrefix(const TableIdent &tbl_id,
                                      std::string_view filename,
                                      size_t prefix_len,
                                      DirectIoBuffer &buffer,
                                      size_t dst_offset)
{
    if (prefix_len == 0)
    {
        return KvError::NoError;
    }
    if (dst_offset > buffer.size() || prefix_len > buffer.size() - dst_offset)
    {
        LOG(ERROR) << "Invalid prefix destination range, table=" << tbl_id
                   << " filename=" << filename << " prefix_len=" << prefix_len
                   << " dst_offset=" << dst_offset
                   << " buffer_size=" << buffer.size();
        return KvError::InvalidArgs;
    }

    // Construct absolute path to file
    fs::path abs_path =
        tbl_id.StorePath(options_->store_path, options_->store_path_lut);
    abs_path /= filename;

    // Open file for reading using io_uring
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    open_how how = {.flags = O_RDONLY | O_NOATIME, .mode = 0, .resolve = 0};
    io_uring_prep_openat2(sqe, AT_FDCWD, abs_path.c_str(), &how);
    int fd = ThdTask()->WaitIoResult();
    if (fd < 0)
    {
        LOG(ERROR) << "Failed to open file prefix for upload, table=" << tbl_id
                   << " filename=" << filename << " error=" << strerror(-fd);
        return ToKvError(fd);
    }

    // Read prefix_len bytes from file start in larger batches to reduce
    // per-IO overhead on shard threads.
    const size_t read_batch_size = options_->non_page_io_batch_size;

    FdIdx fd_idx{fd, false};
    KvError status = KvError::NoError;
    size_t remaining = prefix_len;
    size_t read_offset = 0;
    while (remaining > 0)
    {
        size_t batch = std::min(read_batch_size, remaining);
        int read_res = Read(fd_idx,
                            buffer.data() + dst_offset + read_offset,
                            batch,
                            read_offset);
        if (read_res < 0)
        {
            status = ToKvError(read_res);
            LOG(ERROR) << "Failed to read file prefix for upload, table="
                       << tbl_id << " filename=" << filename
                       << " offset=" << read_offset
                       << " error=" << strerror(-read_res);
            break;
        }
        if (read_res == 0)
        {
            // Unexpected EOF: file is shorter than expected
            status = KvError::EndOfFile;
            LOG(ERROR) << "Unexpected EOF while reading file prefix, table="
                       << tbl_id << " filename=" << filename
                       << " offset=" << read_offset
                       << " expected=" << prefix_len;
            break;
        }

        read_offset += static_cast<size_t>(read_res);
        remaining -= static_cast<size_t>(read_res);
    }

    // Close file descriptor
    sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_close(sqe, fd);
    int close_res = ThdTask()->WaitIoResult();
    if (close_res < 0)
    {
        LOG(ERROR) << "Failed to close prefix-read file, table=" << tbl_id
                   << " filename=" << filename
                   << " error=" << strerror(-close_res);
        // Don't override read error with close error
        if (status == KvError::NoError)
        {
            status = ToKvError(close_res);
        }
    }
    CHECK_KV_ERR(status);
    return KvError::NoError;
}

KvError CloudStoreMgr::ReadFilePrefix(const TableIdent &tbl_id,
                                      std::string_view filename,
                                      size_t prefix_len,
                                      DirectIoBuffer &buffer,
                                      size_t dst_offset,
                                      FdIdx cached_fd)
{
    if (prefix_len == 0)
    {
        return KvError::NoError;
    }
    if (dst_offset > buffer.size() || prefix_len > buffer.size() - dst_offset)
    {
        LOG(ERROR) << "Invalid prefix destination range, table=" << tbl_id
                   << " filename=" << filename << " prefix_len=" << prefix_len
                   << " dst_offset=" << dst_offset
                   << " buffer_size=" << buffer.size();
        return KvError::InvalidArgs;
    }

    // Read prefix_len bytes from file start using the cached FD (which
    // references the inode directly, immune to path-level rename races).
    const size_t read_batch_size = options_->non_page_io_batch_size;

    KvError status = KvError::NoError;
    size_t remaining = prefix_len;
    size_t read_offset = 0;
    while (remaining > 0)
    {
        size_t batch = std::min(read_batch_size, remaining);
        int read_res = Read(cached_fd,
                            buffer.data() + dst_offset + read_offset,
                            batch,
                            read_offset);
        if (read_res < 0)
        {
            status = ToKvError(read_res);
            LOG(ERROR) << "Failed to read file prefix (cached FD) for upload, "
                       << "table=" << tbl_id << " filename=" << filename
                       << " offset=" << read_offset
                       << " error=" << strerror(-read_res);
            break;
        }
        if (read_res == 0)
        {
            // Unexpected EOF: file is shorter than expected
            status = KvError::EndOfFile;
            LOG(ERROR) << "Unexpected EOF reading file prefix (cached FD), "
                       << "table=" << tbl_id << " filename=" << filename
                       << " offset=" << read_offset
                       << " expected=" << prefix_len;
            break;
        }

        read_offset += static_cast<size_t>(read_res);
        remaining -= static_cast<size_t>(read_res);
    }

    CHECK_KV_ERR(status);
    return KvError::NoError;
}

KvError CloudStoreMgr::RestoreLocalCacheState()
{
    // Scan each shard-owned partition directory and rebuild the closed-file
    // LRU so that pre-existing files participate in eviction accounting.
    size_t restored_files = 0;
    size_t restored_bytes = 0;
    const uint16_t num_shards = options_->num_threads;

    for (const std::string &root_path_str : options_->store_path)
    {
        const fs::path root_path(root_path_str);
        std::error_code ec;
        fs::directory_iterator partition_it(root_path, ec);
        if (ec)
        {
            LOG(ERROR) << "Failed to scan store path " << root_path << ": "
                       << ec.message();
            return ToKvError(-ec.value());
        }

        fs::directory_iterator end;
        for (; partition_it != end; partition_it.increment(ec))
        {
            if (ec)
            {
                LOG(ERROR) << "Failed to iterate store path " << root_path
                           << ": " << ec.message();
                return ToKvError(-ec.value());
            }

            const std::string entry_name =
                partition_it->path().filename().string();
            if (entry_name == "lost+found")
            {
                continue;
            }
            std::error_code type_ec;
            if (!partition_it->is_directory(type_ec) || type_ec)
            {
                LOG(ERROR) << "Invalid files exist in store path: "
                           << entry_name;
                return KvError::InvalidArgs;
            }

            TableIdent tbl_id = TableIdent::FromString(
                partition_it->path().filename().string());
            if (!tbl_id.IsValid())
            {
                LOG(ERROR) << "Invalid files exist in store path: "
                           << entry_name;
                return KvError::InvalidArgs;
            }
            if (tbl_id.ShardIndex(num_shards) != shard_id_)
            {
                continue;
            }

            KvError err = RestoreFilesForTable(
                tbl_id, partition_it->path(), restored_files, restored_bytes);
            CHECK_KV_ERR(err);
        }

        if (ec)
        {
            LOG(ERROR) << "Failed to iterate store path " << root_path << ": "
                       << ec.message();
            return ToKvError(-ec.value());
        }
    }

    auto [trimmed_files, trimmed_bytes] = TrimRestoredCacheUsage();
    if (trimmed_files > 0)
    {
        LOG(INFO) << "Shard " << shard_id_ << " evicted " << trimmed_files
                  << " cached files (" << trimmed_bytes
                  << " bytes) while honoring the restored cache budget";
    }

    size_t retained_files =
        restored_files > trimmed_files ? restored_files - trimmed_files : 0;
    size_t retained_bytes =
        restored_bytes > trimmed_bytes ? restored_bytes - trimmed_bytes : 0;
    if (retained_files > 0)
    {
        LOG(INFO) << "Shard " << shard_id_ << " reused " << retained_files
                  << " local files (" << retained_bytes
                  << " bytes) when starting";
    }

    return KvError::NoError;
}

KvError CloudStoreMgr::RestoreFilesForTable(const TableIdent &tbl_id,
                                            const fs::path &table_path,
                                            size_t &restored_files,
                                            size_t &restored_bytes)
{
    // Register all cacheable files that already exist within one partition
    // directory and bump the tracked local space usage accordingly.
    std::error_code ec;
    fs::directory_iterator file_it(table_path, ec);
    if (ec)
    {
        LOG(ERROR) << "Failed to list partition directory " << table_path
                   << " for table " << tbl_id << ": " << ec.message();
        return ToKvError(-ec.value());
    }

    struct CachedFileInfo
    {
        std::string filename;
        fs::path path;
        bool is_data_file;
        size_t expected_size;
        FileId file_id;
        uint64_t term;
    };

    std::vector<CachedFileInfo> cached_files;
    cached_files.reserve(64);

    bool has_max_data_file = false;
    FileId max_file_id = 0;
    std::vector<size_t> max_data_file_indices;

    fs::directory_iterator end;
    for (; file_it != end; file_it.increment(ec))
    {
        if (ec)
        {
            LOG(ERROR) << "Failed to iterate partition directory " << table_path
                       << " for table " << tbl_id << ": " << ec.message();
            return ToKvError(-ec.value());
        }

        std::error_code type_ec;
        if (!file_it->is_regular_file(type_ec) || type_ec)
        {
            LOG(ERROR) << "Unexpected entry " << file_it->path()
                       << " in partition directory " << table_path
                       << ": not a regular file";
            return KvError::InvalidArgs;
        }

        std::string filename = file_it->path().filename().string();
        if (filename.empty() ||
            boost::algorithm::ends_with(filename, TmpSuffix))
        {
            fs::path unexpected_path = file_it->path();
            std::error_code remove_ec;
            fs::remove(unexpected_path, remove_ec);
            if (remove_ec)
            {
                LOG(ERROR) << "Failed to remove cached file " << unexpected_path
                           << ": " << remove_ec.message();
            }
            else
            {
                LOG(INFO) << "Removed unexpected cached file "
                          << unexpected_path;
            }
            continue;
        }

        auto [prefix, suffix] = ParseFileName(filename);
        bool is_data_file = prefix == FileNameData;
        bool is_manifest_file = prefix == FileNameManifest;
        if (!is_data_file && !is_manifest_file)
        {
            LOG(ERROR) << "Unknown cached file type " << file_it->path() << " ("
                       << filename << ") encountered during cache restore";
            return KvError::InvalidArgs;
        }

        if (is_manifest_file)
        {
            std::error_code remove_ec;
            fs::remove(file_it->path(), remove_ec);
            if (remove_ec)
            {
                LOG(ERROR) << "Failed to remove manifest file "
                           << file_it->path() << ": " << remove_ec.message();
                return KvError::InvalidArgs;
            }
            else
            {
                LOG(INFO) << "Removed manifest file " << file_it->path()
                          << " during cache restore";
            }
            continue;
        }

        CachedFileInfo info{filename,
                            file_it->path(),
                            is_data_file,
                            EstimateFileSize(filename),
                            0,
                            0};

        if (is_data_file)
        {
            FileId file_id = 0;
            std::string_view branch_name_out;
            uint64_t term = 0;
            if (!ParseDataFileSuffix(suffix, file_id, branch_name_out, term))
            {
                LOG(ERROR) << "Invalid data file name " << info.path
                           << " encountered during cache restore";
                return KvError::InvalidArgs;
            }
            info.file_id = file_id;
            info.term = term;
            if (!has_max_data_file || file_id > max_file_id)
            {
                has_max_data_file = true;
                max_file_id = file_id;
                max_data_file_indices = {cached_files.size()};
            }
            else if (file_id == max_file_id)
            {
                max_data_file_indices.emplace_back(cached_files.size());
            }
        }

        cached_files.emplace_back(std::move(info));
    }

    if (ec)
    {
        LOG(ERROR) << "Failed to iterate partition directory " << table_path
                   << " for table " << tbl_id << ": " << ec.message();
        return ToKvError(-ec.value());
    }

    if (has_max_data_file)
    {
        for (size_t idx : max_data_file_indices)
        {
            const CachedFileInfo &victim = cached_files[idx];
            std::error_code remove_ec;
            fs::remove(victim.path, remove_ec);
            if (remove_ec)
            {
                LOG(ERROR) << "Failed to remove max data file " << victim.path
                           << ": " << remove_ec.message();
                return KvError::InvalidArgs;
            }

            LOG(INFO) << "Removed max data file " << victim.path
                      << " during cache restore";
        }
    }

    const size_t retained_files =
        cached_files.size() - max_data_file_indices.size();
    for (size_t i = 0; i < cached_files.size(); ++i)
    {
        if (has_max_data_file && std::find(max_data_file_indices.begin(),
                                           max_data_file_indices.end(),
                                           i) != max_data_file_indices.end())
        {
            continue;
        }

        CachedFileInfo &file_info = cached_files[i];
        EnqueClosedFile(FileKey{tbl_id, std::move(file_info.filename)});
        used_local_space_ += file_info.expected_size;
        ++restored_files;
        restored_bytes += file_info.expected_size;
    }

    if (retained_files == 0)
    {
        std::error_code remove_ec;
        bool removed_dir = fs::remove(table_path, remove_ec);
        if (remove_ec)
        {
            if (remove_ec.value() != ENOENT && remove_ec.value() != ENOTEMPTY &&
                remove_ec.value() != EEXIST)
            {
                LOG(ERROR) << "Failed to remove idle partition directory "
                           << table_path << " for table " << tbl_id << ": "
                           << remove_ec.message();
                return ToKvError(-remove_ec.value());
            }
        }
        else if (removed_dir)
        {
            LOG(INFO) << "Removed idle partition directory " << table_path
                      << " during cache restore";
        }
    }

    return KvError::NoError;
}

size_t CloudStoreMgr::GetLocalSpaceUsed() const
{
    return used_local_space_;
}

size_t CloudStoreMgr::GetLocalSpaceLimit() const
{
    return shard_local_space_limit_;
}

DirectIoBufferPool &CloudStoreMgr::GetDirectIoBufferPool()
{
    return direct_io_buffer_pool_;
}

std::pair<size_t, size_t> CloudStoreMgr::TrimRestoredCacheUsage()
{
    size_t trimmed_files = 0;
    size_t trimmed_bytes = 0;

    while (used_local_space_ > shard_local_space_limit_ && HasEvictableFile())
    {
        CachedFile *victim = lru_file_tail_.prev_;
        if (victim == &lru_file_head_)
        {
            break;
        }

        FileKey key_copy = *victim->key_;
        fs::path file_path = key_copy.tbl_id_.StorePath(
            options_->store_path, options_->store_path_lut);
        file_path /= key_copy.filename_;

        std::error_code ec;
        fs::remove(file_path, ec);
        if (ec)
        {
            LOG(WARNING) << "Failed to remove cached file " << file_path
                         << " during restore: " << ec.message();
            break;
        }

        size_t file_size = EstimateFileSize(key_copy.filename_);
        victim->Deque();
        DecrementClosedFileCount(key_copy.tbl_id_);
        closed_files_.erase(key_copy);
        used_local_space_ =
            used_local_space_ > file_size ? used_local_space_ - file_size : 0;
        ++trimmed_files;
        trimmed_bytes += file_size;
    }

    if (used_local_space_ > shard_local_space_limit_)
    {
        LOG(WARNING) << "Local cache usage " << used_local_space_
                     << " still exceeds limit " << shard_local_space_limit_
                     << " after restore";
    }

    return {trimmed_files, trimmed_bytes};
}

bool CloudStoreMgr::IsIdle()
{
    return file_cleaner_.status_ == TaskStatus::Idle &&
           pending_gc_cleanup_.empty() && active_prewarm_tasks_ == 0 &&
           inflight_cloud_slots_ == 0 && !obj_store_.HasPendingWork();
}

void CloudStoreMgr::Stop()
{
    file_cleaner_.Shutdown();
    for (auto &prewarmer : prewarmers_)
    {
        prewarmer->Shutdown();
    }
    if (cloud_service_ && !IsStoreStopping())
    {
        WaitForCloudTasksToDrain();
    }
    // AsyncHttpManager containers can be touched by cloud worker threads via
    // ObjectStore::RunHttpWork(). Drain all cloud tasks first so Shutdown()
    // runs after workers finish in-flight work on this object store.
    obj_store_.Shutdown();
}

bool CloudStoreMgr::NeedPrewarm() const
{
    return options_->prewarm_cloud_cache && HasPrewarmPending();
}

void CloudStoreMgr::RunPrewarm()
{
    if (prewarmers_.empty() || !HasPrewarmPending())
    {
        return;
    }
    for (auto &prewarmer : prewarmers_)
    {
        prewarmer->stop_.store(false, std::memory_order_release);
        prewarmer->Resume();
    }
}

void CloudStoreMgr::RegisterPrewarmActive()
{
    ++active_prewarm_tasks_;
}

void CloudStoreMgr::UnregisterPrewarmActive()
{
    CHECK_GT(active_prewarm_tasks_, 0);
    --active_prewarm_tasks_;
}

bool CloudStoreMgr::HasPrewarmPending() const
{
    return prewarm_queue_size_.load(std::memory_order_acquire) > 0;
}

bool CloudStoreMgr::PopPrewarmFile(PrewarmFile &file)
{
    if (!prewarm_queue_.try_dequeue(file))
    {
        return false;
    }
    prewarm_queue_size_.fetch_sub(1, std::memory_order_acq_rel);
    prewarm_files_pulled_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void CloudStoreMgr::ClearPrewarmFiles()
{
    PrewarmFile dummy;
    while (prewarm_queue_.try_dequeue(dummy))
    {
    }
    prewarm_queue_size_.store(0, std::memory_order_release);
}

void CloudStoreMgr::StopAllPrewarmTasks()
{
    for (auto &prewarmer : prewarmers_)
    {
        prewarmer->stop_.store(true, std::memory_order_release);
    }
}

void CloudStoreMgr::WaitForCloudTasksToDrain()
{
    while (obj_store_.HasPendingWork() || inflight_cloud_slots_ > 0)
    {
#ifdef ELOQ_MODULE_ENABLED
        constexpr uint64_t kPollIntervalUs = 5000;
        bthread_usleep(kPollIntervalUs);
#else
        constexpr auto kPollInterval = std::chrono::milliseconds(5);
        std::this_thread::sleep_for(kPollInterval);
#endif
    }
}

void CloudStoreMgr::AcquireCloudSlot(KvTask *task)
{
    if (task == nullptr)
    {
        return;
    }
    while (inflight_cloud_slots_ >= Options()->max_cloud_concurrency)
    {
        cloud_slot_waiting_.Wait(task);
    }
    ++inflight_cloud_slots_;
}

void CloudStoreMgr::ReleaseCloudSlot(size_t count)
{
    CHECK_GE(inflight_cloud_slots_, count);
    inflight_cloud_slots_ -= count;
    cloud_slot_waiting_.WakeN(count);
}

void CloudStoreMgr::EnqueueCloudReadyTask(ObjectStore::Task *task)
{
    cloud_ready_tasks_.enqueue(task);
}

void CloudStoreMgr::ProcessCloudReadyTasks(Shard *shard)
{
    if (shard == nullptr)
    {
        return;
    }
    ObjectStore::Task *ready_tasks[128];
    CHECK(shard != nullptr);
    size_t nready = cloud_ready_tasks_.try_dequeue_bulk(ready_tasks,
                                                        std::size(ready_tasks));
    if (nready == 0)
    {
        return;
    }

    for (size_t i = 0; i < nready; ++i)
    {
        ready_tasks[i]->CompleteCloudTask();
    }

    ReleaseCloudSlot(nready);
}

bool CloudStoreMgr::AppendPrewarmFiles(std::vector<PrewarmFile> &files)
{
    if (files.empty())
    {
        return true;
    }

#ifdef ELOQ_MODULE_ENABLED
    eloq::EloqModule::NotifyWorker(shard_id_);
#endif

    while (!prewarm_listing_complete_.load(std::memory_order_acquire))
    {
        size_t current_size =
            prewarm_queue_size_.load(std::memory_order_acquire);
        if (current_size < kMaxPrewarmPendingFiles)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (prewarm_listing_complete_.load(std::memory_order_acquire))
    {
        return false;
    }

    size_t num_files = files.size();
    prewarm_queue_.enqueue_bulk(std::make_move_iterator(files.begin()),
                                num_files);
    size_t prev_size =
        prewarm_queue_size_.fetch_add(num_files, std::memory_order_release);
    files.clear();

    if (prev_size == 0)
    {
        for (auto &prewarmer : prewarmers_)
        {
            prewarmer->stop_.store(false, std::memory_order_release);
        }
    }
    return true;
}

size_t CloudStoreMgr::GetPrewarmPendingCount() const
{
    return prewarm_queue_size_.load(std::memory_order_acquire);
}

void CloudStoreMgr::MarkPrewarmListingComplete()
{
    prewarm_listing_complete_.store(true, std::memory_order_release);
}

bool CloudStoreMgr::IsPrewarmListingComplete() const
{
    return prewarm_listing_complete_.load(std::memory_order_acquire);
}

size_t CloudStoreMgr::GetPrewarmFilesPulled() const
{
    return prewarm_files_pulled_.load(std::memory_order_relaxed);
}

std::pair<ManifestFilePtr, KvError> CloudStoreMgr::GetManifest(
    const TableIdent &tbl_id)
{
    // If the manifest is already downloaded, return it.
    auto [manifest, err] = IouringMgr::GetManifest(tbl_id);
    if (err == KvError::NoError)
    {
        LOG(INFO) << "CloudStoreMgr::GetManifest: manifest already downloaded "
                  << "for table " << tbl_id;
        return {std::move(manifest), err};
    }

    uint64_t process_term = ProcessTerm();
    // Use active branch for all local manifest filenames in this function.
    std::string active_br = std::string(GetActiveBranch());

    // Term file is updated at startup only (BootstrapUpsertTermFile).

    KvError dl_err =
        DownloadFile(tbl_id, LruFD::kManifest, active_br, process_term, false);
    if (dl_err == KvError::NoError)
    {
        return IouringMgr::GetManifest(tbl_id);
    }
    else if (dl_err != KvError::NotFound)
    {
        // Hard error when trying to fetch expected term.
        LOG(ERROR) << "CloudStoreMgr::GetManifest: failed to download "
                   << "manifest for term " << process_term << " : "
                   << ErrorString(dl_err);
        return {nullptr, dl_err};
    }
    // NotFound: fall through to list & pick latest manifest.

    // List manifests in cloud and pick the term (ignoring archive files).
    // If there is manifest term bigger than process_term, return error.
    // Else select the manifest that term equals or less than process_term.

    uint64_t selected_term = 0;
    std::string selected_branch;
    std::vector<std::string> cloud_files;
    // List all manifest files under this table path.
    // (Notice: file names in list response will not contain "manifest_"
    // prefix.)
    std::string remote_path =
        tbl_id.ToString() + "/" + FileNameManifest + FileNameSeparator;

    // Loop to fetch all pages of results (S3 ListObjectsV2 returns max 1000
    // per page)
    std::string continuation_token;
    KvTask *current_task = ThdTask();
    do
    {
        ObjectStore::ListTask list_task(remote_path, false);
        list_task.SetContinuationToken(continuation_token);
        list_task.SetKvTask(current_task);
        AcquireCloudSlot(current_task);
        obj_store_.SubmitTask(&list_task, shard);
        current_task->WaitIo();

        if (list_task.error_ != KvError::NoError)
        {
            LOG(ERROR) << "CloudStoreMgr::GetManifest: list objects failed "
                          "for "
                       << tbl_id << " : " << ErrorString(list_task.error_);
            return {nullptr, list_task.error_};
        }

        std::vector<std::string> batch_files;
        std::string next_token;
        if (!obj_store_.ParseListObjectsResponse(
                list_task.response_data_.view(),
                list_task.json_data_,
                &batch_files,
                nullptr,
                &next_token))
        {
            LOG(ERROR) << "CloudStoreMgr::GetManifest: parse list response "
                          "failed for table "
                       << tbl_id;
            return {nullptr, KvError::Corrupted};
        }

        // Append batch results to the main list
        cloud_files.insert(cloud_files.end(),
                           std::make_move_iterator(batch_files.begin()),
                           std::make_move_iterator(batch_files.end()));

        // Continue with next page if there is a continuation token
        continuation_token = std::move(next_token);
    } while (!continuation_token.empty());

    if (cloud_files.empty() ||
        (cloud_files.size() == 1 && cloud_files[0] == CurrentTermFileName))
    {
        // No manifest at all in cloud.
        return {nullptr, KvError::NotFound};
    }

    uint64_t best_term = 0;
    bool found = false;
    for (const std::string &name : cloud_files)
    {
        // "name" does not contain the prefix("manifest_").
        std::string_view branch_name;
        uint64_t term = 0;
        std::optional<std::string> tag;
        if (!ParseManifestFileSuffix(name, branch_name, term, tag))
        {
            LOG(FATAL) << "CloudStoreMgr::GetManifest: failed to parse "
                          "manifest file suffix: "
                       << name;
            continue;
        }

        // Skip archive manifests (manifest_<term>_<tag>).
        if (tag.has_value())
        {
            continue;
        }

        if (term >= best_term)
        {
            found = true;
            best_term = term;
            selected_branch = std::string(branch_name);
        }
    }

    if (!found)
    {
        // No manifest found; this partition does not belong to the current
        // restore database.
        return {nullptr, KvError::NotFound};
    }

    selected_term = best_term;
    // selected_term < process_term: allowed; allocator bump will be
    // handled by Replayer when it sees term mismatch.
    if (selected_term > process_term)
    {
        LOG(ERROR) << "CloudStoreMgr::GetManifest: found manifest term "
                   << selected_term << " greater than process_term "
                   << process_term << " for table " << tbl_id;
        return {nullptr, KvError::ExpiredTerm};
    }

    // Ensure the selected manifest is downloaded locally.
    dl_err = DownloadFile(
        tbl_id, LruFD::kManifest, selected_branch, selected_term, false);
    if (dl_err != KvError::NoError)
    {
        LOG(ERROR) << "CloudStoreMgr::GetManifest: failed to download "
                   << "selected manifest term " << selected_term
                   << " for table " << tbl_id << " : " << ErrorString(dl_err);
        return {nullptr, dl_err};
    }

    // If ProcessTerm() is set and the selected term is older than
    // process_term, "promote" the manifest: copy its content into a new
    // manifest_<branch>_<process_term> object (both locally and in cloud), so
    // subsequent readers can consistently use manifest_<branch>_<process_term>.
    if (selected_term != process_term)
    {
        // 1) Rename the manifest file locally.
        auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
        if (err != KvError::NoError)
        {
            LOG(ERROR) << "CloudStoreMgr::GetManifest: failed to open "
                       << "directory for table " << tbl_id << " : "
                       << ErrorString(err);
            return {nullptr, err};
        }

        std::string src_filename =
            BranchManifestFileName(selected_branch, selected_term);
        std::string promoted_name =
            BranchManifestFileName(active_br, process_term);
        int res = Rename(
            dir_fd.FdPair(), src_filename.c_str(), promoted_name.c_str());
        if (res < 0)
        {
            LOG(ERROR) << "CloudStoreMgr::GetManifest: failed to rename "
                       << "manifest file " << src_filename << " to "
                       << promoted_name << " for table " << tbl_id << " : "
                       << strerror(-res);
            return {nullptr, ToKvError(res)};
        }
        res = Fdatasync(dir_fd.FdPair());
        if (res < 0)
        {
            LOG(ERROR) << "fsync directory failed " << strerror(-res);
            return {nullptr, ToKvError(res)};
        }

        // 2) Upload manifest_<branch>_<process_term> to cloud.
        // (No need to delete the manifest file if failed to upload. The content
        // of this manifest is same to the one on cloud and can be used on this
        // read operation (without changing manifest content). The manifest with
        // process_term will be uploaded on next write operation.)
        KvError up_err = UploadFile(tbl_id, promoted_name, nullptr);
        if (up_err != KvError::NoError)
        {
            LOG(ERROR) << "CloudStoreMgr::GetManifest: failed to upload "
                       << "promoted manifest file " << promoted_name
                       << " for table " << tbl_id << " : "
                       << ErrorString(up_err);
        }
        // Update manifest_branch_term_ to the promoted term/branch.
    }
    else
    {
        // No promotion needed; selected branch/term is active_br/process_term.
    }

    // Delegate to base implementation to open the local manifest file and
    // build ManifestFile wrapper.
    return IouringMgr::GetManifest(tbl_id);
}

namespace
{
class BufferManifest final : public ManifestFile
{
public:
    explicit BufferManifest(std::string_view content) : content_(content)
    {
    }

    KvError Read(char *dst, size_t n) override
    {
        if (content_.size() < n)
        {
            return KvError::EndOfFile;
        }
        std::memcpy(dst, content_.data(), n);
        content_.remove_prefix(n);
        return KvError::NoError;
    }

    KvError SkipPadding(size_t n) override
    {
        if (content_.size() < n)
        {
            return KvError::EndOfFile;
        }
        content_.remove_prefix(n);
        return KvError::NoError;
    }

private:
    std::string_view content_;
};
}  // namespace

std::pair<ManifestFilePtr, KvError> CloudStoreMgr::RefreshManifest(
    const TableIdent &tbl_id, std::string_view archive_tag)
{
    if (archive_tag.empty())
    {
        // Always fetch the latest manifest from cloud, even if local exists.
        LruFD::Ref old_fd = GetOpenedFD(tbl_id, LruFD::kManifest);
        if (old_fd != nullptr)
        {
            CloseFile(std::move(old_fd));
        }

        uint64_t process_term = ProcessTerm();
        uint64_t selected_term = process_term;
        DirectIoBuffer buffer;
        auto download_to_buffer = [&](uint64_t term) -> KvError
        {
            KvTask *current_task = ThdTask();
            std::string filename =
                BranchManifestFileName(GetActiveBranch(), term);
            ObjectStore::DownloadTask download_task(&tbl_id, filename);
            download_task.SetKvTask(current_task);
            download_task.response_data_ =
                std::move(direct_io_buffer_pool_.Acquire());
            AcquireCloudSlot(current_task);
            obj_store_.SubmitTask(&download_task, shard);
            current_task->WaitIo();

            if (download_task.error_ != KvError::NoError)
            {
                RecycleBuffer(std::move(download_task.response_data_));
                return download_task.error_;
            }
            buffer = std::move(download_task.response_data_);
            return KvError::NoError;
        };

        KvError dl_err = download_to_buffer(process_term);
        if (dl_err == KvError::NotFound)
        {
            uint64_t best_term = 0;
            bool found = false;
            std::vector<std::string> cloud_files;
            std::string remote_path =
                tbl_id.ToString() + "/" + FileNameManifest + FileNameSeparator;

            std::string continuation_token;
            KvTask *current_task = ThdTask();
            do
            {
                ObjectStore::ListTask list_task(remote_path, false);
                list_task.SetContinuationToken(continuation_token);
                list_task.SetKvTask(current_task);
                AcquireCloudSlot(current_task);
                obj_store_.SubmitTask(&list_task, shard);
                current_task->WaitIo();

                if (list_task.error_ != KvError::NoError)
                {
                    LOG(ERROR)
                        << "CloudStoreMgr::RefreshManifest: list objects "
                           "failed for "
                        << tbl_id << " : " << ErrorString(list_task.error_);
                    return {nullptr, list_task.error_};
                }

                std::vector<std::string> batch_files;
                std::string next_token;
                if (!obj_store_.ParseListObjectsResponse(
                        list_task.response_data_.view(),
                        list_task.json_data_,
                        &batch_files,
                        nullptr,
                        &next_token))
                {
                    LOG(ERROR) << "CloudStoreMgr::RefreshManifest: parse list "
                                  "response failed for table "
                               << tbl_id;
                    return {nullptr, KvError::Corrupted};
                }

                cloud_files.insert(cloud_files.end(),
                                   std::make_move_iterator(batch_files.begin()),
                                   std::make_move_iterator(batch_files.end()));
                continuation_token = std::move(next_token);
            } while (!continuation_token.empty());

            if (cloud_files.empty() || (cloud_files.size() == 1 &&
                                        cloud_files[0] == CurrentTermFileName))
            {
                return {nullptr, KvError::NotFound};
            }

            for (const std::string &name : cloud_files)
            {
                uint64_t term = 0;
                std::string_view branch_name;
                std::optional<std::string> tag;
                if (!ParseManifestFileSuffix(name, branch_name, term, tag))
                {
                    LOG(FATAL) << "CloudStoreMgr::RefreshManifest: failed to "
                                  "parse manifest file suffix: "
                               << name;
                    continue;
                }
                if (tag.has_value())
                {
                    continue;
                }
                if (term >= best_term)
                {
                    found = true;
                    best_term = term;
                }
            }

            if (!found)
            {
                return {nullptr, KvError::NotFound};
            }
            selected_term = best_term;
            dl_err = download_to_buffer(selected_term);
        }

        if (dl_err != KvError::NoError)
        {
            return {nullptr, dl_err};
        }

        BufferManifest buffer_manifest(buffer.view());
        Replayer replayer(options_);
        KvError replay_err = replayer.Replay(&buffer_manifest);
        if (replay_err != KvError::NoError)
        {
            RecycleBuffer(std::move(buffer));
            return {nullptr, replay_err};
        }

        std::string tmp_name =
            BranchManifestFileName(GetActiveBranch(), selected_term) + ".tmp";
        uint64_t flags = O_WRONLY | O_CREAT | O_DIRECT | O_NOATIME | O_TRUNC;
        KvError write_err = WriteFile(tbl_id, tmp_name, buffer, flags);
        RecycleBuffer(std::move(buffer));
        if (write_err != KvError::NoError)
        {
            return {nullptr, write_err};
        }

        auto [dir_fd, dir_err] =
            OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
        if (dir_err != KvError::NoError)
        {
            return {nullptr, dir_err};
        }

        std::string manifest_name =
            BranchManifestFileName(GetActiveBranch(), selected_term);
        int res =
            Rename(dir_fd.FdPair(), tmp_name.c_str(), manifest_name.c_str());
        if (res < 0)
        {
            return {nullptr, ToKvError(res)};
        }
        res = Fdatasync(dir_fd.FdPair());
        if (res < 0)
        {
            return {nullptr, ToKvError(res)};
        }

        if (selected_term != process_term)
        {
            std::string promoted_name =
                BranchManifestFileName(GetActiveBranch(), process_term);
            res = Rename(
                dir_fd.FdPair(), manifest_name.c_str(), promoted_name.c_str());
            if (res < 0)
            {
                return {nullptr, ToKvError(res)};
            }
            res = Fdatasync(dir_fd.FdPair());
            if (res < 0)
            {
                return {nullptr, ToKvError(res)};
            }
        }

        return IouringMgr::GetManifest(tbl_id);
    }

    LruFD::Ref old_fd = GetOpenedFD(tbl_id, LruFD::kManifest);
    if (old_fd != nullptr)
    {
        CloseFile(std::move(old_fd));
    }

    uint64_t process_term = ProcessTerm();
    uint64_t selected_term = process_term;
    std::string selected_filename =
        BranchArchiveName(GetActiveBranch(), selected_term, archive_tag);
    DirectIoBuffer buffer;
    auto download_to_buffer = [&](std::string_view filename) -> KvError
    {
        KvTask *current_task = ThdTask();
        ObjectStore::DownloadTask download_task(&tbl_id, std::string(filename));
        download_task.SetKvTask(current_task);
        download_task.response_data_ = AcquireCloudBuffer(current_task);
        AcquireCloudSlot(current_task);
        obj_store_.SubmitTask(&download_task, shard);
        current_task->WaitIo();

        if (download_task.error_ != KvError::NoError)
        {
            ReleaseCloudBuffer(std::move(download_task.response_data_));
            return download_task.error_;
        }
        buffer = std::move(download_task.response_data_);
        return KvError::NoError;
    };

    KvError dl_err = download_to_buffer(selected_filename);

    if (dl_err != KvError::NoError)
    {
        return {nullptr, dl_err};
    }

    BufferManifest buffer_manifest(buffer.view());
    Replayer replayer(options_);
    KvError replay_err = replayer.Replay(&buffer_manifest);
    if (replay_err != KvError::NoError)
    {
        ReleaseCloudBuffer(std::move(buffer));
        return {nullptr, replay_err};
    }

    std::string manifest_name =
        BranchManifestFileName(GetActiveBranch(), selected_term);
    std::string tmp_name = manifest_name + ".tmp";
    uint64_t flags = O_WRONLY | O_CREAT | O_DIRECT | O_NOATIME | O_TRUNC;
    KvError write_err = WriteFile(tbl_id, tmp_name, buffer, flags);
    ReleaseCloudBuffer(std::move(buffer));
    if (write_err != KvError::NoError)
    {
        return {nullptr, write_err};
    }

    auto [dir_fd, dir_err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    if (dir_err != KvError::NoError)
    {
        return {nullptr, dir_err};
    }

    int res = Rename(dir_fd.FdPair(), tmp_name.c_str(), manifest_name.c_str());
    if (res < 0)
    {
        return {nullptr, ToKvError(res)};
    }
    res = Fdatasync(dir_fd.FdPair());
    if (res < 0)
    {
        return {nullptr, ToKvError(res)};
    }

    if (selected_term != process_term)
    {
        std::string promoted_name =
            BranchManifestFileName(GetActiveBranch(), process_term);
        res = Rename(
            dir_fd.FdPair(), manifest_name.c_str(), promoted_name.c_str());
        if (res < 0)
        {
            return {nullptr, ToKvError(res)};
        }
        res = Fdatasync(dir_fd.FdPair());
        if (res < 0)
        {
            return {nullptr, ToKvError(res)};
        }
    }

    return IouringMgr::GetManifest(tbl_id);
}

std::tuple<uint64_t, std::string, KvError> CloudStoreMgr::ReadTermFile(
    std::string_view branch_name)
{
    KvTask *current_task = ThdTask();

    // Download CURRENT_TERM_<branch>_<pg_id> file from store root
    std::string term_filename = CurrentTermFileNameForBranchAndPartitionGroup(
        branch_name, partition_group_id_);
    ObjectStore::DownloadTask download_task(term_filename);
    download_task.SetKvTask(current_task);
    AcquireCloudSlot(current_task);
    obj_store_.SubmitTask(&download_task, shard);
    current_task->WaitIo();

    if (download_task.error_ == KvError::NotFound)
    {
        return {0, "", KvError::NotFound};
    }
    if (download_task.error_ != KvError::NoError)
    {
        return {0, "", download_task.error_};
    }

    // Parse term value from response_data_ (trim whitespace first)
    std::string_view term_str = download_task.response_data_.view();
    // Trim leading/trailing whitespace
    while (!term_str.empty() &&
           (term_str.front() == ' ' || term_str.front() == '\t' ||
            term_str.front() == '\r' || term_str.front() == '\n'))
    {
        term_str.remove_prefix(1);
    }
    while (!term_str.empty() &&
           (term_str.back() == ' ' || term_str.back() == '\t' ||
            term_str.back() == '\r' || term_str.back() == '\n'))
    {
        term_str.remove_suffix(1);
    }
    uint64_t term = 0;
    try
    {
        term = std::stoull(std::string(term_str));
    }
    catch (const std::exception &)
    {
        return {0, "", KvError::Corrupted};
    }

    return {term, download_task.etag_, KvError::NoError};
}

KvError CloudStoreMgr::CleanupLocalPartitionFiles(const TableIdent &tbl_id)
{
    namespace fs = std::filesystem;

    const fs::path table_path =
        tbl_id.StorePath(options_->store_path, options_->store_path_lut);
    std::error_code ec;
    if (!fs::exists(table_path, ec))
    {
        return ec ? ToKvError(-ec.value()) : KvError::NoError;
    }

    std::vector<std::string> cleanup_targets;
    for (fs::directory_iterator it(table_path, ec), end; it != end;
         it.increment(ec))
    {
        if (ec)
        {
            return ToKvError(-ec.value());
        }
        const fs::file_status status = it->status(ec);
        if (ec)
        {
            return ToKvError(-ec.value());
        }
        if (!fs::is_regular_file(status))
        {
            continue;
        }

        const std::string filename = it->path().filename().string();
        auto [prefix, suffix] = ParseFileName(filename);
        static_cast<void>(suffix);
        if (prefix == FileNameData)
        {
            cleanup_targets.push_back(filename);
        }
    }

    CHECK(shard != nullptr);
    bool empty_mapping = false;
    RootMetaMgr *root_meta_mgr = shard->IndexManager()->RootMetaManager();
    auto *entry = root_meta_mgr->Find(tbl_id);
    if (entry != nullptr)
    {
        RootMeta &meta = entry->meta_;
        empty_mapping = meta.mapper_ != nullptr && meta.root_id_ == MaxPageId &&
                        meta.ttl_root_id_ == MaxPageId &&
                        meta.mapper_->MappingCount() == 0;
    }

    if (empty_mapping)
    {
        const std::string manifest_name =
            BranchManifestFileName(GetActiveBranch(), ProcessTerm());
        const fs::path manifest_path = table_path / manifest_name;
        if (fs::exists(manifest_path, ec) && !ec)
        {
            FileKey manifest_key{tbl_id, manifest_name};
            if (!closed_files_.contains(manifest_key))
            {
                EnqueClosedFile(manifest_key);
            }
            cleanup_targets.push_back(manifest_name);
        }
    }

    if (!cleanup_targets.empty())
    {
        ScheduleLocalFileCleanup(tbl_id, cleanup_targets);
    }
    else
    {
        return TryCleanupLocalPartitionDir(tbl_id);
    }

    return KvError::NoError;
}

void CloudStoreMgr::ScheduleLocalFileCleanup(
    const TableIdent &tbl_id, const std::vector<std::string> &filenames)
{
    bool queued = false;
    for (const std::string &filename : filenames)
    {
        FileKey key(tbl_id, filename);
        if (!closed_files_.contains(key) || IsEvictingKey(key))
        {
            continue;
        }
        auto [_, inserted] = pending_gc_cleanup_.insert(key);
        if (!inserted)
        {
            continue;
        }
        queued = true;
    }

    if (queued && file_cleaner_.status_ == TaskStatus::Idle)
    {
        file_cleaner_.Resume();
    }
}

void CloudStoreMgr::RegisterDirBusy(const TableIdent &tbl_id)
{
    dir_busy_counts_[tbl_id]++;
}

void CloudStoreMgr::UnregisterDirBusy(const TableIdent &tbl_id)
{
    auto it = dir_busy_counts_.find(tbl_id);
    if (it == dir_busy_counts_.end())
    {
        return;
    }
    if (--it->second == 0)
    {
        dir_busy_counts_.erase(it);
        if (!HasTrackedLocalFiles(tbl_id))
        {
            pending_dir_cleanup_.push_back(tbl_id);
            if (file_cleaner_.status_ == TaskStatus::Idle)
            {
                file_cleaner_.Resume();
            }
        }
    }
}

bool CloudStoreMgr::HasDirBusy(const TableIdent &tbl_id) const
{
    auto it = dir_busy_counts_.find(tbl_id);
    return it != dir_busy_counts_.end() && it->second > 0;
}

bool CloudStoreMgr::IsDirEvicting(const TableIdent &tbl_id) const
{
    return evicting_paths_.contains(
        EvictingPathKey(tbl_id, LruFD::kDirectory, 0));
}

KvError CloudStoreMgr::SwitchManifest(const TableIdent &tbl_id,
                                      std::string_view snapshot)
{
    LruFD::Ref fd_ref = GetOpenedFD(tbl_id, LruFD::kManifest);
    if (fd_ref != nullptr)
    {
        // Close the old manifest firstly.
        KvError err = CloseFile(std::move(fd_ref));
        CHECK_KV_ERR(err);
    }

    // We have to prevent the new generated manifest from being removed by LRU
    // mechanism after renamed but before uploaded.
    // Always update manifest branch/term from current active branch + process
    // term.
    std::string_view active_br = GetActiveBranch();
    uint64_t manifest_term_val = ProcessTerm();
    const std::string manifest_name =
        BranchManifestFileName(active_br, manifest_term_val);
    FileKey fkey{tbl_id, manifest_name};
    bool dequed = DequeClosedFile(fkey);
    if (!dequed)
    {
        // Manifest is not cached locally, we need to reserve cache space for
        // this manifest.
        int res = ReserveCacheSpace(options_->manifest_limit);
        if (res < 0)
        {
            return ToKvError(res);
        }
    }

    auto [dir_fd, err] =
        OpenOrCreateFD(tbl_id, LruFD::kDirectory, false, true, "", 0);
    if (err != KvError::NoError)
    {
        if (dequed)
        {
            EnqueClosedFile(fkey);
        }
        LOG(ERROR) << "Cloud SwitchManifest open dir failed for " << tbl_id
                   << " manifest_term=" << manifest_term_val << ": "
                   << ErrorString(err);
        return err;
    }
    int res = WriteSnapshot(std::move(dir_fd), manifest_name, snapshot);
    if (res < 0)
    {
        LOG(ERROR) << "Cloud SwitchManifest WriteSnapshot failed for " << tbl_id
                   << " manifest=" << manifest_name << " : " << strerror(-res);
        if (dequed)
        {
            EnqueClosedFile(fkey);
        }
        return ToKvError(res);
    }

    if (!dequed)
    {
        // This is a new cached file.
        used_local_space_ += options_->manifest_limit;
    }
    err = UploadFile(tbl_id, manifest_name, nullptr, snapshot);
    if (err != KvError::NoError)
    {
        LOG(FATAL) << "can not upload manifest: " << ErrorString(err);
    }
    IouringMgr::CloseDirect(res);
    EnqueClosedFile(std::move(fkey));
    return KvError::NoError;
}

KvError CloudStoreMgr::CleanManifest(const TableIdent &tbl_id)
{
    KvError err = IouringMgr::CleanManifest(tbl_id);
    if (err != KvError::NoError)
    {
        return err;
    }
    if (DequeClosedFile(FileKey(
            tbl_id, BranchManifestFileName(GetActiveBranch(), ProcessTerm()))))
    {
        const size_t manifest_size = options_->manifest_limit;
        used_local_space_ = used_local_space_ > manifest_size
                                ? used_local_space_ - manifest_size
                                : 0;
    }
    if (!HasTrackedLocalFiles(tbl_id))
    {
        KvError cleanup_err = TryCleanupLocalPartitionDir(tbl_id);
        if (cleanup_err != KvError::NoError)
        {
            LOG(WARNING) << "Failed to clean partition directory for table "
                         << tbl_id << ": " << ErrorString(cleanup_err);
            return cleanup_err;
        }
    }
    return KvError::NoError;
}

KvError CloudStoreMgr::CreateArchive(const TableIdent &tbl_id,
                                     std::string_view branch_name,
                                     uint64_t term,
                                     std::string_view snapshot,
                                     std::string_view tag)
{
    FileKey key(tbl_id, BranchArchiveName(branch_name, term, tag));
    bool dequed = DequeClosedFile(key);
    if (dequed)
    {
        LOG(WARNING) << "CreateArchive found existing archive in closed cache, "
                        "table="
                     << tbl_id << " filename=" << key.filename_;
    }
    if (!dequed)
    {
        int res = ReserveCacheSpace(options_->manifest_limit);
        if (res < 0)
        {
            return ToKvError(res);
        }
    }

    auto [dir_fd, err] =
        OpenOrCreateFD(tbl_id, LruFD::kDirectory, false, true, "", 0);
    if (err != KvError::NoError)
    {
        if (dequed)
        {
            EnqueClosedFile(key);
        }
        return err;
    }
    int res = WriteSnapshot(std::move(dir_fd), key.filename_, snapshot);
    if (res < 0)
    {
        if (dequed)
        {
            EnqueClosedFile(std::move(key));
        }
        return ToKvError(res);
    }

    err = UploadFile(tbl_id, key.filename_, nullptr, snapshot);
    IouringMgr::CloseDirect(res);
    if (!dequed)
    {
        used_local_space_ += options_->manifest_limit;
    }
    EnqueClosedFile(std::move(key));
    return err;
}

KvError CloudStoreMgr::DeleteArchive(const TableIdent &tbl_id,
                                     std::string_view branch_name,
                                     uint64_t term,
                                     std::string_view tag)
{
    if (term == std::numeric_limits<uint64_t>::max())
    {
        term = ProcessTerm();
    }
    const std::string name = BranchArchiveName(branch_name, term, tag);
    auto [dir_fd, open_err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    CHECK_KV_ERR(open_err);
    const int local_unlink_res = UnlinkAt(dir_fd.FdPair(), name.c_str(), false);
    if (local_unlink_res < 0 && local_unlink_res != -ENOENT)
    {
        return ToKvError(local_unlink_res);
    }

    KvTask *current_task = ThdTask();
    ObjectStore::DeleteTask delete_task(tbl_id.ToString() + "/" + name);
    delete_task.SetKvTask(current_task);
    AcquireCloudSlot(current_task);
    obj_store_.SubmitTask(&delete_task, shard);
    current_task->WaitIo();
    if (delete_task.error_ == KvError::NotFound)
    {
        return KvError::NoError;
    }
    return delete_task.error_;
}

KvError CloudStoreMgr::WriteBranchManifest(const TableIdent &tbl_id,
                                           std::string_view branch_name,
                                           uint64_t term,
                                           std::string_view snapshot)
{
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    CHECK_KV_ERR(err);

    // Generate branch manifest filename: manifest_<branch_name>_<term>
    const std::string name = BranchManifestFileName(branch_name, term);

    int res = WriteSnapshot(std::move(dir_fd), name, snapshot);
    if (res < 0)
    {
        return ToKvError(res);
    }
    err = UploadFile(tbl_id, name, nullptr, snapshot);
    IouringMgr::CloseDirect(res);
    return err;
}

KvError CloudStoreMgr::DeleteBranchFiles(const TableIdent &tbl_id,
                                         std::string_view branch_name,
                                         uint64_t /* term (unused) */)
{
    std::string normalized_branch = NormalizeBranchName(branch_name);
    std::string current_term_filename =
        CurrentTermFileNameForBranchAndPartitionGroup(branch_name,
                                                      partition_group_id_);

    // Helper lambda: list all cloud objects under a given prefix and append
    // matching paths to paths_to_delete.  The full object path stored in
    // paths_to_delete is: prefix + returned_suffix (ParseListObjectsResponse
    // strips the prefix from every returned key so we reconstruct it here).
    std::vector<std::string> paths_to_delete;

    auto list_and_collect =
        [&](const std::string &prefix,
            std::function<bool(const std::string &suffix)> predicate)
    {
        std::string continuation_token;
        KvTask *list_task_owner = ThdTask();
        do
        {
            ObjectStore::ListTask list_task(prefix, false);
            list_task.SetContinuationToken(continuation_token);
            list_task.SetRecursive(true);
            list_task.SetKvTask(list_task_owner);
            AcquireCloudSlot(list_task_owner);
            obj_store_.SubmitTask(&list_task, shard);
            list_task_owner->WaitIo();

            if (list_task.error_ != KvError::NoError)
            {
                LOG(WARNING) << "DeleteBranchFiles: list failed for prefix "
                             << prefix << ": " << ErrorString(list_task.error_);
                break;
            }

            std::vector<std::string> batch_files;
            std::string next_token;
            if (!obj_store_.ParseListObjectsResponse(
                    list_task.response_data_.view(),
                    list_task.json_data_,
                    &batch_files,
                    nullptr,
                    &next_token))
            {
                LOG(WARNING) << "DeleteBranchFiles: parse list response failed "
                             << "for prefix " << prefix;
                break;
            }

            for (const std::string &suffix : batch_files)
            {
                if (predicate(suffix))
                {
                    paths_to_delete.push_back(prefix + suffix);
                }
            }

            continuation_token = std::move(next_token);
        } while (!continuation_token.empty());
    };

    // 1. Collect all manifest_<branch>_<term> objects by listing with the
    //    exact branch-specific prefix.  This is reliable regardless of whether
    //    CURRENT_TERM.<branch> is up-to-date, and handles term gaps correctly.
    {
        // Prefix covers exactly "manifest_<branch>_" — no other branch can
        // share this prefix because branch names are unique and normalized.
        std::string manifest_prefix =
            tbl_id.ToString() + "/" + std::string(FileNameManifest) +
            std::string(1, FileNameSeparator) + normalized_branch +
            std::string(1, FileNameSeparator);
        list_and_collect(manifest_prefix,
                         [](const std::string &) { return true; });
    }

    // 2. Always include CURRENT_TERM_<branch>_<pg_id> at store root (may or
    //    may not exist; the delete task is idempotent — NotFound is silently
    //    ignored by the object store delete path).
    paths_to_delete.push_back(current_term_filename);

    // 3. Collect all data_<file_id>_<branch>_<term> objects by listing the
    //    "data_" prefix and filtering for the branch marker "_<branch>_".
    {
        std::string branch_marker = std::string(1, FileNameSeparator) +
                                    normalized_branch +
                                    std::string(1, FileNameSeparator);
        std::string data_prefix = tbl_id.ToString() + "/" +
                                  std::string(FileNameData) +
                                  std::string(1, FileNameSeparator);
        list_and_collect(
            data_prefix,
            [&](const std::string &suffix)
            { return suffix.find(branch_marker) != std::string::npos; });
    }

    KvTask *current_task = ThdTask();
    std::vector<ObjectStore::DeleteTask> delete_tasks;
    delete_tasks.reserve(paths_to_delete.size());

    for (const std::string &path : paths_to_delete)
    {
        delete_tasks.emplace_back(path);
        delete_tasks.back().SetKvTask(current_task);
        AcquireCloudSlot(current_task);
        obj_store_.SubmitTask(&delete_tasks.back(), shard);
        current_task->WaitIo();
    }

    // Check for cloud delete failures.
    KvError cloud_err = KvError::NoError;
    for (const auto &task : delete_tasks)
    {
        if (task.error_ != KvError::NoError)
        {
            LOG(ERROR) << "DeleteBranchFiles: failed to delete cloud object "
                       << task.remote_path_ << ": " << ErrorString(task.error_);
            if (cloud_err == KvError::NoError)
            {
                cloud_err = task.error_;
            }
        }
    }

    // Clean up local cache files for this branch so they don't linger until
    // space-pressure eviction.  IouringMgr::DeleteBranchFiles silently
    // ignores ENOENT, so it is safe to call even if nothing was cached.
    KvError local_err = IouringMgr::DeleteBranchFiles(tbl_id, branch_name, 0);
    if (local_err != KvError::NoError)
    {
        LOG(WARNING) << "DeleteBranchFiles: failed to remove local cache files "
                     << "for branch " << branch_name << ": "
                     << static_cast<int>(local_err);
        // Non-fatal: stale local files will eventually be evicted by the LRU
        // cache.
    }

    return cloud_err;
}

KvError CloudStoreMgr::AbortWrite(const TableIdent &tbl_id)
{
    // First abort the base I/O manager state (reset dirty flags, etc.)
    KvError err = IouringMgr::AbortWrite(tbl_id);
    CHECK_KV_ERR(err);
    return KvError::NoError;
}

int CloudStoreMgr::CreateFile(LruFD::Ref dir_fd,
                              FileId file_id,
                              std::string_view branch_name,
                              uint64_t term)
{
    size_t size = options_->DataFileSize();
    int res = ReserveCacheSpace(size);
    if (res < 0)
    {
        return res;
    }
    res = IouringMgr::CreateFile(std::move(dir_fd), file_id, branch_name, term);
    if (res >= 0)
    {
        used_local_space_ += size;
    }
    return res;
}

int CloudStoreMgr::OpenFile(const TableIdent &tbl_id,
                            FileId file_id,
                            uint64_t flags,
                            uint64_t mode,
                            std::string_view branch_name,
                            uint64_t term,
                            bool skip_cloud_lookup)
{
    std::string filename = (file_id == LruFD::kManifest)
                               ? BranchManifestFileName(branch_name, term)
                               : BranchDataFileName(file_id, branch_name, term);
    FileKey key{tbl_id, filename};
    pending_gc_cleanup_.erase(key);
    if (DequeClosedFile(key))
    {
        // Try to open the file cached locally.
        int res = IouringMgr::OpenFile(
            tbl_id, file_id, flags, mode, branch_name, term);
        if (res < 0 && res != -ENOENT)
        {
            EnqueClosedFile(std::move(key));
        }
        return res;
    }

    if (skip_cloud_lookup)
    {
        return -ENOENT;
    }

    // File not exists locally, try to download it from cloud.
    size_t size = EstimateFileSize(file_id);
    int res = ReserveCacheSpace(size);
    if (res < 0)
    {
        return res;
    }
    KvError err = DownloadFile(tbl_id, file_id, branch_name, term, false);
    switch (err)
    {
    case KvError::NoError:
        used_local_space_ += size;
        break;
    case KvError::NotFound:
        return -ENOENT;
    case KvError::OutOfSpace:
        return -ENOSPC;
    case KvError::TryAgain:
        return -EAGAIN;
    default:
        return -EIO;
    }

    // Try to open the successfully downloaded file.
    res = IouringMgr::OpenFile(tbl_id, file_id, flags, mode, branch_name, term);
    if (res < 0 && res != -ENOENT)
    {
        EnqueClosedFile(std::move(key));
    }
    return res;
}

KvError CloudStoreMgr::SyncFile(LruFD::Ref fd)
{
    FileId file_id = fd.Get()->file_id_;
    KvError err = KvError::NoError;
    if (file_id != LruFD::kDirectory)
    {
        const TableIdent &tbl_id = *fd.Get()->tbl_->tbl_id_;
        uint64_t term = fd.Get()->term_;
        std::string filename;
        if (file_id == LruFD::kManifest)
        {
            // Use branch-aware manifest filename for both local read and cloud
            // upload key, consistent with SwitchManifest.
            filename = BranchManifestFileName(GetActiveBranch(), ProcessTerm());
        }
        else
        {
            std::string_view branch = fd.Get()->branch_name_;
            filename = BranchDataFileName(file_id, branch, term);
            DLOG(INFO) << "SyncFile data, tbl=" << tbl_id
                       << " file_id=" << file_id << " fd_branch=" << branch
                       << " fd_term=" << term << " filename=" << filename
                       << " reg_idx=" << fd.Get()->reg_idx_;
        }
        err = UploadFile(
            tbl_id, filename, CurrentWriteTask(), {}, true, fd.FdPair());
        if (file_id == LruFD::kManifest)
        {
            // For manifest files, retry until success or error that
            // means manifest is not uploaded (insufficient storage).
            // Manifests are critical and must be uploaded eventually.
            while (err != KvError::NoError &&
                   err != KvError::OssInsufficientStorage)
            {
                LOG(WARNING) << "Manifest upload failed with "
                             << ErrorString(err) << ", retrying.";
                err = UploadFile(tbl_id,
                                 filename,
                                 CurrentWriteTask(),
                                 {},
                                 true,
                                 fd.FdPair());
            }
        }
        if (err == KvError::NoError)
        {
            fd.Get()->dirty_ = false;
        }
        else
        {
            LOG(ERROR) << "Failed to upload file " << filename << ": "
                       << ErrorString(err);
        }
        return err;
    }
    return err;
}

KvError CloudStoreMgr::SyncFiles(const TableIdent &tbl_id,
                                 std::span<LruFD::Ref> fds)
{
    if (options_->data_append_mode && CurrentWriteTask() != nullptr)
    {
        size_t dirty_data_files = 0;
        for (LruFD::Ref fd : fds)
        {
            FileId file_id = fd.Get()->file_id_;
            if (file_id <= LruFD::kMaxDataFile)
            {
                ++dirty_data_files;
            }
        }
        if (dirty_data_files > 1)
        {
            LOG(ERROR) << "SyncFiles found more than one dirty data file in a "
                          "single write task, table="
                       << tbl_id << " dirty_data_files=" << dirty_data_files;
            return KvError::Corrupted;
        }
    }

    std::vector<std::pair<std::string, FdIdx>> files;
    for (LruFD::Ref fd : fds)
    {
        FileId file_id = fd.Get()->file_id_;
        if (file_id != LruFD::kDirectory)
        {
            uint64_t term = fd.Get()->term_;
            std::string filename;
            if (file_id == LruFD::kManifest)
            {
                filename =
                    BranchManifestFileName(GetActiveBranch(), ProcessTerm());
            }
            else
            {
                std::string_view branch = fd.Get()->branch_name_;
                filename = BranchDataFileName(file_id, branch, term);
            }
            files.emplace_back(std::move(filename), fd.FdPair());
        }
    }
    KvError err = UploadFiles(tbl_id, std::move(files));
    if (err != KvError::NoError)
    {
        return err;
    }

    for (LruFD::Ref &fd_ref : fds)
    {
        if (auto *fd = fd_ref.Get())
        {
            fd->dirty_ = false;
        }
    }

    return KvError::NoError;
}

KvError CloudStoreMgr::CloseFile(LruFD::Ref fd)
{
    FileId file_id = fd.Get()->file_id_;
    std::optional<FileKey> file_key;

    if (file_id != LruFD::kDirectory)
    {
        uint64_t term = fd.Get()->term_;
        std::string_view branch = fd.Get()->branch_name_;
        std::string filename;
        if (file_id == LruFD::kManifest)
        {
            filename = BranchManifestFileName(branch, term);
        }
        else
        {
            filename = BranchDataFileName(file_id, branch, term);
        }
        file_key.emplace(*fd.Get()->tbl_->tbl_id_, filename);
    }

    KvError err = IouringMgr::CloseFile(fd);
    CHECK_KV_ERR(err);
    if (file_key.has_value())
    {
        EnqueClosedFile(*file_key);
        if (pending_gc_cleanup_.contains(*file_key) &&
            file_cleaner_.status_ == TaskStatus::Idle)
        {
            file_cleaner_.Resume();
        }
    }
    return KvError::NoError;
}

size_t CloudStoreMgr::EstimateFileSize(FileId file_id) const
{
    if (file_id == LruFD::kManifest)
    {
        return options_->manifest_limit;
    }
    else
    {
        assert(file_id <= LruFD::kMaxDataFile);
        return options_->DataFileSize();
    }
}

size_t CloudStoreMgr::EstimateFileSize(std::string_view filename) const
{
    switch (filename.front())
    {
    case 'd':  // data_xxx
        return options_->DataFileSize();
    case 'm':  // manifest or manifest_xxx
        return options_->manifest_limit;
    default:
        LOG(FATAL) << "Unknown file type: " << filename;
    }
    __builtin_unreachable();
}

FileKey CloudStoreMgr::EvictingPathKey(const TableIdent &tbl_id,
                                       FileId file_id,
                                       uint64_t term) const
{
    if (file_id == LruFD::kDirectory)
    {
        return FileKey(tbl_id, std::string(kDirectoryEvictingPath));
    }
    if (file_id == LruFD::kManifest)
    {
        return FileKey(tbl_id, BranchManifestFileName(GetActiveBranch(), term));
    }
    return FileKey(tbl_id,
                   BranchDataFileName(file_id, GetActiveBranch(), term));
}

void CloudStoreMgr::WaitForEvictingPath(const TableIdent &tbl_id,
                                        FileId file_id,
                                        uint64_t term)
{
    WaitForEvictingKey(EvictingPathKey(tbl_id, file_id, term));
}

bool CloudStoreMgr::StartEvictingPath(const TableIdent &tbl_id,
                                      FileId file_id,
                                      uint64_t term)
{
    return StartEvictingKey(EvictingPathKey(tbl_id, file_id, term));
}

void CloudStoreMgr::FinishEvictingPath(const TableIdent &tbl_id,
                                       FileId file_id,
                                       uint64_t term)
{
    FinishEvictingKey(EvictingPathKey(tbl_id, file_id, term));
}

void CloudStoreMgr::WaitForEvictingKey(const FileKey &key)
{
    auto it = evicting_paths_.find(key);
    while (it != evicting_paths_.end())
    {
        it->second.waiting_.Wait(ThdTask());
        it = evicting_paths_.find(key);
    }
}

bool CloudStoreMgr::StartEvictingKey(FileKey key)
{
    auto [it, inserted] = evicting_paths_.try_emplace(std::move(key));
    return inserted;
}

void CloudStoreMgr::FinishEvictingKey(const FileKey &key)
{
    auto it = evicting_paths_.find(key);
    if (it == evicting_paths_.end())
    {
        return;
    }
    it->second.waiting_.WakeAll();
    evicting_paths_.erase(it);
}

bool CloudStoreMgr::IsEvictingKey(const FileKey &key) const
{
    return evicting_paths_.contains(key);
}

void CloudStoreMgr::InitBackgroundJob()
{
    IouringMgr::InitBackgroundJob();
    shard->running_ = &file_cleaner_;
    file_cleaner_.coro_ = boost::context::callcc(
        [this](continuation &&sink)
        {
            shard->main_ = std::move(sink);
            file_cleaner_.Run();
            return std::move(shard->main_);
        });
    if (options_->prewarm_cloud_cache)
    {
        for (auto &prewarmer : prewarmers_)
        {
            shard->running_ = prewarmer.get();
            prewarmer->coro_ = boost::context::callcc(
                [this, prewarmer_ptr = prewarmer.get()](continuation &&sink)
                {
                    shard->main_ = std::move(sink);
                    prewarmer_ptr->Run();
                    return std::move(shard->main_);
                });
        }
    }
}

bool CloudStoreMgr::DequeClosedFile(const FileKey &key)
{
    WaitForEvictingKey(key);
    pending_gc_cleanup_.erase(key);
    auto it = closed_files_.find(key);
    if (it == closed_files_.end())
    {
        return false;
    }
    CachedFile *local_file = &it->second;
    local_file->Deque();
    DecrementClosedFileCount(key.tbl_id_);
    closed_files_.erase(it);
    return true;
}

void CloudStoreMgr::EnqueClosedFile(FileKey key)
{
    assert(key.tbl_id_.IsValid());
    auto [it, ok] = closed_files_.try_emplace(std::move(key));
    assert(ok);
    it->second.key_ = &it->first;
    IncrementClosedFileCount(it->first.tbl_id_);
    lru_file_head_.EnqueNext(&it->second);
}

void CloudStoreMgr::IncrementClosedFileCount(const TableIdent &tbl_id)
{
    closed_file_counts_[tbl_id]++;
}

void CloudStoreMgr::DecrementClosedFileCount(const TableIdent &tbl_id)
{
    auto it = closed_file_counts_.find(tbl_id);
    CHECK(it != closed_file_counts_.end());
    CHECK_GT(it->second, 0U);
    if (--it->second == 0)
    {
        closed_file_counts_.erase(it);
    }
}

bool CloudStoreMgr::HasTrackedLocalFiles(const TableIdent &tbl_id) const
{
    if (closed_file_counts_.contains(tbl_id))
    {
        return true;
    }

    auto it = tables_.find(tbl_id);
    if (it == tables_.end())
    {
        return false;
    }

    const auto &fds = it->second.fds_;
    CHECK(!fds.empty()) << "tables_ contains empty PartitionFiles for table "
                        << tbl_id;
    if (fds.size() > 1)
    {
        return true;
    }

    return fds.begin()->first != LruFD::kDirectory;
}

bool CloudStoreMgr::HasEvictableFile() const
{
    return lru_file_tail_.prev_ != &lru_file_head_;
}

int CloudStoreMgr::ReserveCacheSpace(size_t size)
{
    KvTask *current_task = ThdTask();
    while (used_local_space_ + size > shard_local_space_limit_)
    {
        if (!HasEvictableFile())
        {
            LOG(WARNING) << "Cannot reserve " << size
                         << " bytes: used=" << used_local_space_
                         << " limit=" << shard_local_space_limit_
                         << " no evictable files" << " task=" << current_task;
            return -ENOSPC;
        }

        if (file_cleaner_.status_ == TaskStatus::Idle)
        {
            // Wake up file cleaner to evict files.
            file_cleaner_.Resume();
        }
        // This task will be woken up by file cleaner.
        file_cleaner_.requesting_.Wait(ThdTask());
    }
    return 0;
}

KvError CloudStoreMgr::DownloadFile(const TableIdent &tbl_id,
                                    FileId file_id,
                                    std::string_view branch_name,
                                    uint64_t term,
                                    bool download_to_exist,
                                    uint64_t offset)
{
    KvTask *current_task = ThdTask();
    std::string filename = (file_id == LruFD::kManifest)
                               ? BranchManifestFileName(branch_name, term)
                               : BranchDataFileName(file_id, branch_name, term);

    ObjectStore::DownloadTask download_task(&tbl_id, filename);

    // Set KvTask pointer and initialize inflight_io_
    download_task.SetKvTask(current_task);
    download_task.response_data_ = AcquireCloudBuffer(current_task);

    AcquireCloudSlot(current_task);
    obj_store_.SubmitTask(&download_task, shard);
    current_task->WaitIo();

    if (download_task.error_ != KvError::NoError)
    {
        ReleaseCloudBuffer(std::move(download_task.response_data_));
        return download_task.error_;
    }

    auto [dir_fd, dir_err] =
        OpenOrCreateFD(tbl_id, LruFD::kDirectory, false, true, "", 0);
    if (dir_err != KvError::NoError)
    {
        ReleaseCloudBuffer(std::move(download_task.response_data_));
        return dir_err;
    }

    std::string tmp_filename = filename + ".tmp";

    if (download_to_exist)
    {
        // Rename the existing file away before overwriting, so readers see
        // either the old complete file or the new complete file (never a
        // partial write).  ENOENT is fine — the file may not exist yet.
        int res =
            Rename(dir_fd.FdPair(), filename.c_str(), tmp_filename.c_str());
        if (res != 0 && res != -ENOENT)
        {
            ReleaseCloudBuffer(std::move(download_task.response_data_));
            return ToKvError(res);
        }
    }

    uint64_t flags = O_WRONLY | O_CREAT | O_DIRECT | O_NOATIME;
    KvError err = WriteFile(
        tbl_id, tmp_filename, download_task.response_data_, flags, offset);
    ReleaseCloudBuffer(std::move(download_task.response_data_));
    CHECK_KV_ERR(err);

    int res = Rename(dir_fd.FdPair(), tmp_filename.c_str(), filename.c_str());
    if (res < 0)
    {
        return ToKvError(res);
    }

    return KvError::NoError;
}

StandbyStoreMgr::StandbyStoreMgr(const KvOptions *opts, uint32_t fd_limit)
    : IouringMgr(opts, fd_limit)
{
    CHECK(opts != nullptr);
    const std::string &addr = opts->standby_master_addr;
    if (addr.empty())
        return;
    remote_addr_ = addr;
    LOG(INFO) << "StandbyStoreMgr replicating from " << remote_addr_;
}

void StandbyStoreMgr::Stop()
{
    if (!IsStoreStopping())
    {
        WaitForStandbyTasksToDrain();
    }
}

void StandbyStoreMgr::WaitForStandbyTasksToDrain()
{
    while (inflight_standby_tasks_.load(std::memory_order_acquire) > 0)
    {
#ifdef ELOQ_MODULE_ENABLED
        constexpr uint64_t kPollIntervalUs = 5000;
        bthread_usleep(kPollIntervalUs);
#else
        constexpr auto kPollInterval = std::chrono::milliseconds(5);
        std::this_thread::sleep_for(kPollInterval);
#endif
    }
}

std::pair<ManifestFilePtr, KvError> StandbyStoreMgr::GetManifest(
    const TableIdent &tbl_id)
{
    auto [manifest, err] = IouringMgr::GetManifest(tbl_id);
    if (err != KvError::NotFound)
    {
        if (err != KvError::NoError)
        {
            DLOG(INFO) << "StandbyStoreMgr::GetManifest direct load failed for "
                       << tbl_id << ", process_term=" << ProcessTerm()
                       << ", err=" << ErrorString(err);
        }
        return {std::move(manifest), err};
    }
    StandbyService *standby_service = shard->store_->GetStandbyService();
    if (standby_service == nullptr)
    {
        return {nullptr, KvError::InvalidArgs};
    }
    KvTask *current_task = ThdTask();
    if (current_task == nullptr)
    {
        return {nullptr, KvError::InvalidArgs};
    }

    const uint64_t target_term = ProcessTerm();
    uint64_t source_term = 0;
    KvError prep_err = standby_service->PrepareLocalManifest(
        tbl_id, target_term, &source_term);
    if (prep_err != KvError::NoError)
    {
        return {nullptr, prep_err};
    }
    current_task->WaitIo();
    prep_err = static_cast<KvError>(current_task->io_res_);
    if (prep_err != KvError::NoError)
    {
        return {nullptr, prep_err};
    }
    DLOG(INFO) << "StandbyStoreMgr::GetManifest fallback prepared manifest_"
               << target_term << " from source term " << source_term << " for "
               << tbl_id;
    return IouringMgr::GetManifest(tbl_id);
}

std::string StandbyStoreMgr::BuildRemoteFilePath(
    const TableIdent &tbl_id, std::string_view filename) const
{
    size_t remote_path_idx =
        tbl_id.StorePathIndex(options_->standby_master_store_paths.size(),
                              options_->standby_master_store_path_lut);
    CHECK_LT(remote_path_idx, options_->standby_master_store_paths.size());
    std::string remote_path =
        options_->standby_master_store_paths[remote_path_idx];
    while (remote_path.size() > 1 && remote_path.back() == '/')
    {
        remote_path.pop_back();
    }
    if (!remote_path.empty() && remote_path.back() != '/')
    {
        remote_path.push_back('/');
    }
    remote_path.append(tbl_id.ToString());
    remote_path.push_back('/');
    remote_path.append(filename);
    std::string remote;
    if (remote_addr_.empty() || remote_addr_ == "local")
    {
        remote = std::move(remote_path);
    }
    else
    {
        remote = remote_addr_;
        remote.push_back(':');
        remote.append(remote_path);
    }
    return remote;
}

int StandbyStoreMgr::RunRsync(const std::string &remote, const std::string &dst)
{
    constexpr auto kPollInterval = std::chrono::milliseconds(100);
    constexpr auto kRsyncTimeout = std::chrono::minutes(5);

    inflight_standby_tasks_.fetch_add(1, std::memory_order_acq_rel);
    struct InflightTaskGuard
    {
        std::atomic<size_t> *counter;
        ~InflightTaskGuard()
        {
            counter->fetch_sub(1, std::memory_order_acq_rel);
        }
    } guard{&inflight_standby_tasks_};

    char *const argv[] = {const_cast<char *>("rsync"),
                          const_cast<char *>("-a"),
                          const_cast<char *>("-z"),
                          const_cast<char *>("--inplace"),
                          const_cast<char *>(remote.c_str()),
                          const_cast<char *>(dst.c_str()),
                          nullptr};
    pid_t pid = 0;
    int rc = posix_spawnp(&pid, "rsync", nullptr, nullptr, argv, environ);
    if (rc != 0)
    {
        LOG(ERROR) << "posix_spawnp rsync failed: " << strerror(rc);
        return -1;
    }

    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + kRsyncTimeout;
    while (true)
    {
        int wp = waitpid(pid, &status, WNOHANG);
        if (wp == pid)
        {
            break;
        }
        if (wp < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            LOG(ERROR) << "waitpid for rsync failed: " << strerror(errno);
            return -1;
        }

        if (std::chrono::steady_clock::now() >= deadline)
        {
            if (kill(pid, SIGKILL) < 0 && errno != ESRCH)
            {
                LOG(ERROR) << "failed to kill timed out rsync process: "
                           << strerror(errno);
            }
            while (waitpid(pid, &status, 0) < 0)
            {
                if (errno != EINTR)
                {
                    LOG(ERROR) << "waitpid after timed out rsync failed: "
                               << strerror(errno);
                    break;
                }
            }
            LOG(ERROR) << "rsync timed out after "
                       << std::chrono::duration_cast<std::chrono::milliseconds>(
                              kRsyncTimeout)
                              .count()
                       << "ms";
            return -1;
        }
        std::this_thread::sleep_for(kPollInterval);
    }
    if (WIFEXITED(status))
    {
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status))
    {
        return -WTERMSIG(status);
    }
    return -1;
}

std::pair<ManifestFilePtr, KvError> StandbyStoreMgr::RefreshManifest(
    const TableIdent &tbl_id)
{
    LruFD::Ref old_fd = GetOpenedFD(tbl_id, LruFD::kManifest);
    if (old_fd != nullptr)
    {
        CloseFile(std::move(old_fd));
    }

    fs::path table_dir =
        tbl_id.StorePath(options_->store_path, options_->store_path_lut);
    fs::path manifest_tmp = table_dir / kManifestTmp;
    if (!fs::exists(manifest_tmp))
    {
        LOG(ERROR) << "StandbyStoreMgr: manifest.tmp missing for " << tbl_id;
        return {nullptr, KvError::NotFound};
    }

    uint64_t term = ProcessTerm();
    std::string_view branch = GetActiveBranch();
    fs::path manifest_dst = table_dir / BranchManifestFileName(branch, term);
    auto [dir_fd, dir_err] = OpenFD(tbl_id, LruFD::kDirectory, false, "", 0);
    if (dir_err != KvError::NoError)
    {
        return {nullptr, dir_err};
    }
    int rename_res = Rename(
        dir_fd.FdPair(), kManifestTmp.data(), manifest_dst.filename().c_str());
    if (rename_res < 0)
    {
        LOG(ERROR) << "StandbyStoreMgr: rename manifest failed: "
                   << strerror(-rename_res);
        return {nullptr, ToKvError(rename_res)};
    }
    int sync_res = Fdatasync(dir_fd.FdPair());
    if (sync_res < 0)
    {
        LOG(ERROR) << "StandbyStoreMgr: fsync directory failed: "
                   << strerror(-sync_res);
        return {nullptr, ToKvError(sync_res)};
    }

    SetBranchFileIdTerm(tbl_id, LruFD::kManifest, branch, term);
    return IouringMgr::GetManifest(tbl_id);
}

DirectIoBuffer CloudStoreMgr::AcquireCloudBuffer(KvTask *task)
{
    CHECK(task != nullptr);
    while (options_->direct_io_buffer_pool_size != 0 &&
           inflight_cloud_buffers_ >= options_->direct_io_buffer_pool_size)
    {
        cloud_buffer_waiting_.Wait(task);
    }
    ++inflight_cloud_buffers_;
    return direct_io_buffer_pool_.Acquire();
}

void CloudStoreMgr::ReleaseCloudBuffer(DirectIoBuffer buffer)
{
    if (buffer.capacity() != 0)
    {
        direct_io_buffer_pool_.Release(std::move(buffer));
    }
    CHECK_GT(inflight_cloud_buffers_, 0);
    --inflight_cloud_buffers_;
    cloud_buffer_waiting_.WakeOne();
}

void CloudStoreMgr::RecycleBuffers(std::vector<DirectIoBuffer> &buffers)
{
    for (DirectIoBuffer &buffer : buffers)
    {
        RecycleBuffer(std::move(buffer));
    }
    buffers.clear();
}

void CloudStoreMgr::RecycleBuffer(DirectIoBuffer buffer)
{
    if (buffer.capacity() == 0)
    {
        return;
    }
    direct_io_buffer_pool_.Release(std::move(buffer));
}

KvError IouringMgr::ReadFile(const TableIdent &tbl_id,
                             std::string_view filename,
                             DirectIoBuffer &buffer)
{
    auto [type, id_term_view] = ParseFileName(filename);
    bool is_data_file = type == FileNameData;

    if (!is_data_file && type != FileNameManifest)
    {
        LOG(ERROR) << "Unsupported file for upload: " << filename;
        return KvError::InvalidArgs;
    }

    if (is_data_file)
    {
        FileId file_id = 0;
        std::string_view branch_name;
        uint64_t term = 0;
        if (!ParseDataFileSuffix(id_term_view, file_id, branch_name, term))
        {
            LOG(ERROR) << "Invalid data file name: " << filename;
            return KvError::InvalidArgs;
        }
    }

    fs::path abs_path =
        tbl_id.StorePath(options_->store_path, options_->store_path_lut);
    abs_path /= filename;

    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    open_how how = {
        .flags = O_RDONLY | O_DIRECT | O_NOATIME, .mode = 0, .resolve = 0};
    io_uring_prep_openat2(sqe, AT_FDCWD, abs_path.c_str(), &how);
    int fd = ThdTask()->WaitIoResult();
    if (fd < 0)
    {
        LOG(ERROR) << "Failed to open file for upload: " << abs_path
                   << ", error=" << strerror(-fd);
        return ToKvError(fd);
    }

    size_t file_size = is_data_file ? options_->DataFileSize() : 0;
    if (!is_data_file)
    {
        struct statx stx
        {
        };
        FdIdx stat_fd{fd, false};
        int stat_res = Statx(stat_fd, "", &stx);
        if (stat_res < 0)
        {
            KvError err = ToKvError(stat_res);
            LOG(ERROR) << "Failed to stat file for upload: " << abs_path
                       << ", error=" << strerror(-stat_res);
            if (int close_res = Close(fd); close_res < 0)
            {
                LOG(ERROR) << "Failed to close file after stat: " << abs_path
                           << ", error=" << strerror(-close_res);
            }
            return err;
        }
        file_size = static_cast<size_t>(stx.stx_size);
    }

    buffer.resize(file_size);
    const size_t read_batch_size = Options()->non_page_io_batch_size;
    size_t remaining = buffer.padded_size();
    size_t read_offset = 0;
    FdIdx fd_idx{fd, false};
    KvError status = KvError::NoError;
    while (status == KvError::NoError && remaining > 0)
    {
        size_t batch = std::min(read_batch_size, remaining);
        int read_res =
            Read(fd_idx, buffer.data() + read_offset, batch, read_offset);
        if (read_res < 0)
        {
            status = ToKvError(read_res);
            LOG(ERROR) << "Failed to read file for upload: " << abs_path
                       << ", error=" << strerror(-read_res);
            break;
        }
        if (read_res == 0)
        {
            if (read_offset >= file_size)
            {
                remaining = 0;
            }
            else
            {
                status = KvError::EndOfFile;
                LOG(ERROR) << "Unexpected EOF while reading file for upload: "
                           << abs_path;
            }
            break;
        }
        read_offset += static_cast<size_t>(read_res);
        if (remaining >= static_cast<size_t>(read_res))
        {
            remaining -= static_cast<size_t>(read_res);
        }
        else
        {
            remaining = 0;
        }
        if (read_offset >= file_size)
        {
            remaining = 0;
        }
    }

    sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_close(sqe, fd);
    int res = ThdTask()->WaitIoResult();
    if (res < 0)
    {
        LOG(ERROR) << "close file/directory " << fd
                   << " failed: " << strerror(-res);
        if (status == KvError::NoError)
        {
            status = ToKvError(res);
        }
    }
    CHECK_KV_ERR(status);
    return KvError::NoError;
}

KvError CloudStoreMgr::UploadFile(const TableIdent &tbl_id,
                                  std::string filename,
                                  WriteTask *owner,
                                  std::string_view payload,
                                  bool wait_for_completion,
                                  FdIdx cached_fd)
{
    KvTask *current_task = ThdTask();
    auto upload_task =
        std::make_unique<ObjectStore::UploadTask>(&tbl_id, std::move(filename));
    upload_task->SetKvTask(current_task);
    const bool async_owner_upload = owner != nullptr && !wait_for_completion;

    auto [prefix, suffix] = ParseFileName(upload_task->filename_);
    const bool is_data_file = (prefix == FileNameData);
    if (is_data_file)
    {
        FileId file_id = 0;
        std::string_view branch_name;
        uint64_t term = 0;
        if (!ParseDataFileSuffix(suffix, file_id, branch_name, term))
        {
            LOG(ERROR) << "Invalid data filename for upload: "
                       << upload_task->filename_;
            return KvError::InvalidArgs;
        }
    }
    else if (prefix != FileNameManifest)
    {
        LOG(ERROR) << "Unsupported upload filename: " << upload_task->filename_;
        return KvError::InvalidArgs;
    }

    DirectIoBuffer temp_buffer;
    DirectIoBuffer *upload_buffer = nullptr;
    uint64_t start_offset = 0;
    uint64_t end_offset = 0;
    bool has_buffered_range = false;

    if (owner != nullptr)
    {
        WriteTask::UploadState &state = owner->MutableUploadState();
        if (state.invalid)
        {
            LOG(ERROR) << "WriteTask upload state is invalid, table=" << tbl_id
                       << " filename=" << state.filename;
            owner->ResetUploadState();
            return KvError::InvalidArgs;
        }
        if (state.initialized && state.filename != upload_task->filename_)
        {
            LOG(ERROR) << "WriteTask upload state filename mismatch, table="
                       << tbl_id << " state=" << state.filename
                       << " upload=" << upload_task->filename_;
            owner->ResetUploadState();
            return KvError::Corrupted;
        }
        owner->EnsureUploadStateBuffer();
        upload_buffer = &state.buffer;
        has_buffered_range = state.initialized;
        start_offset = state.start_offset;
        end_offset = state.end_offset;
    }
    else
    {
        temp_buffer = AcquireCloudBuffer(current_task);
        upload_buffer = &temp_buffer;
    }

    auto cleanup = [&]()
    {
        if (owner != nullptr)
        {
            owner->ResetUploadState();
        }
        else
        {
            ReleaseCloudBuffer(std::move(temp_buffer));
        }
    };

    size_t file_size = 0;
    if (has_buffered_range)
    {
        if (end_offset > std::numeric_limits<size_t>::max())
        {
            LOG(ERROR) << "Buffered upload end offset overflow, table="
                       << tbl_id << " filename=" << upload_task->filename_
                       << " end_offset=" << end_offset;
            cleanup();
            return KvError::InvalidArgs;
        }
        file_size = is_data_file ? options_->DataFileSize()
                                 : static_cast<size_t>(end_offset);
    }
    else if (!payload.empty())
    {
        if (owner != nullptr)
        {
            LOG(ERROR) << "Payload upload should not bind a write-task owner, "
                       << "table=" << tbl_id
                       << " filename=" << upload_task->filename_;
            cleanup();
            return KvError::InvalidArgs;
        }
        upload_buffer->assign(payload);
        file_size = upload_buffer->size();
    }
    else
    {
        KvError read_err =
            ReadFile(tbl_id, upload_task->filename_, *upload_buffer);
        if (read_err != KvError::NoError)
        {
            cleanup();
            return read_err;
        }
        file_size = upload_buffer->size();
    }

    if (has_buffered_range)
    {
        if (start_offset > end_offset || end_offset > file_size)
        {
            LOG(ERROR) << "Invalid buffered upload range, table=" << tbl_id
                       << " filename=" << upload_task->filename_
                       << " start_offset=" << start_offset
                       << " end_offset=" << end_offset
                       << " file_size=" << file_size;
            cleanup();
            return KvError::InvalidArgs;
        }
        if (upload_buffer->size() != static_cast<size_t>(end_offset))
        {
            LOG(ERROR) << "Buffered upload size mismatch, table=" << tbl_id
                       << " filename=" << upload_task->filename_
                       << " buffered_size=" << upload_buffer->size()
                       << " end_offset=" << end_offset;
            cleanup();
            return KvError::InvalidArgs;
        }
        upload_buffer->resize(file_size);
        if (start_offset > 0)
        {
            KvError err;
            if (cached_fd.first >= 0)
            {
                // Use the cached FD (inode-based) to avoid a race where a
                // concurrent rename() replaces the file on disk between a
                // write and the subsequent prefix read.
                err = ReadFilePrefix(tbl_id,
                                     upload_task->filename_,
                                     static_cast<size_t>(start_offset),
                                     *upload_buffer,
                                     0,
                                     cached_fd);
            }
            else
            {
                err = ReadFilePrefix(tbl_id,
                                     upload_task->filename_,
                                     static_cast<size_t>(start_offset),
                                     *upload_buffer,
                                     0);
            }
            if (err != KvError::NoError)
            {
                cleanup();
                return err;
            }
        }
    }

    upload_task->file_size_ = file_size;
    upload_task->data_buffer_ = std::move(*upload_buffer);
    if (owner != nullptr)
    {
        owner->ResetUploadState();
    }

    AcquireCloudSlot(current_task);
    if (async_owner_upload)
    {
        upload_task->owner_write_task_ = owner;
    }
    obj_store_.SubmitTask(upload_task.get(), shard);
    if (async_owner_upload)
    {
        owner->AddPendingUploadTask(std::move(upload_task));
        return KvError::NoError;
    }

    current_task->WaitIo();
    KvError upload_err = upload_task->error_;
    ReleaseCloudBuffer(std::move(upload_task->data_buffer_));
    return upload_err;
}

KvError CloudStoreMgr::UploadFiles(
    const TableIdent &tbl_id, std::vector<std::pair<std::string, FdIdx>> files)
{
    WriteTask *owner = CurrentWriteTask();
    for (auto &[filename, cached_fd] : files)
    {
        KvError err =
            UploadFile(tbl_id, std::move(filename), owner, {}, true, cached_fd);
        if (err != KvError::NoError)
        {
            return err;
        }
    }
    return KvError::NoError;
}

KvError CloudStoreMgr::ReadArchiveFileAndDelete(const TableIdent &tbl_id,
                                                const std::string &filename,
                                                DirectIoBuffer &content)
{
    KvError read_err = ReadFile(tbl_id, filename, content);
    if (read_err != KvError::NoError)
    {
        return read_err;
    }

    fs::path local_path =
        tbl_id.StorePath(options_->store_path, options_->store_path_lut);
    local_path /= filename;
    KvError delete_err = DeleteFiles({local_path.string()});
    if (delete_err != KvError::NoError)
    {
        LOG(WARNING) << "Failed to delete file: " << local_path
                     << ", error: " << static_cast<int>(delete_err);
    }

    return KvError::NoError;
}

TaskType CloudStoreMgr::FileCleaner::Type() const
{
    return TaskType::EvictFile;
}

void CloudStoreMgr::FileCleaner::Run()
{
    killed_ = false;
    const KvOptions *opts = io_mgr_->options_;
    const size_t shard_local_space_limit = io_mgr_->shard_local_space_limit_;
    const size_t reserve_space = opts->reserve_space_ratio == 0
                                     ? 0
                                     : double(shard_local_space_limit) /
                                           double(opts->reserve_space_ratio);
    const size_t threshold = shard_local_space_limit - reserve_space;
    // 128 * 8MB (data file) = 1GB
    const uint16_t batch_size = 128;

    struct UnlinkReq : BaseReq
    {
        UnlinkReq() = default;
        CachedFile *file_;
        fs::path path_;
    };

    std::array<UnlinkReq, batch_size> unlink_reqs;
    for (UnlinkReq &req : unlink_reqs)
    {
        req.task_ = this;
    }

    status_ = TaskStatus::Ongoing;
    while (true)
    {
        const bool has_pending_gc = !io_mgr_->pending_gc_cleanup_.empty();
        const bool has_pending_dir_cleanup =
            !io_mgr_->pending_dir_cleanup_.empty();
        const bool needs_pressure_eviction =
            io_mgr_->HasEvictableFile() &&
            (io_mgr_->used_local_space_ > threshold || !requesting_.Empty());
        if (!has_pending_gc && !has_pending_dir_cleanup &&
            !needs_pressure_eviction)
        {
            // No file to evict, or used space is below threshold.
            requesting_.WakeAll();
            status_ = TaskStatus::Idle;
            Yield();
            if (killed_)
            {
                // File cleaner is killed, exit the work loop.
                break;
            }
            status_ = TaskStatus::Ongoing;
            continue;
        }

        uint16_t req_count = 0;
        bool made_progress = false;
        std::unordered_set<TableIdent> touched_partitions;

        if (has_pending_dir_cleanup)
        {
            while (!io_mgr_->pending_dir_cleanup_.empty())
            {
                TableIdent tbl_id =
                    std::move(io_mgr_->pending_dir_cleanup_.front());
                io_mgr_->pending_dir_cleanup_.pop_front();
                made_progress = true;
                KvError cleanup_err =
                    io_mgr_->TryCleanupLocalPartitionDir(tbl_id);
                if (cleanup_err != KvError::NoError)
                {
                    LOG(WARNING)
                        << "Failed to clean local partition directory for "
                        << "table " << tbl_id
                        << " after dir busy count reached zero: "
                        << ErrorString(cleanup_err);
                }
            }
        }

        if (has_pending_gc)
        {
            for (auto it = io_mgr_->pending_gc_cleanup_.begin();
                 it != io_mgr_->pending_gc_cleanup_.end() &&
                 req_count < batch_size;)
            {
                const FileKey &key = *it;

                auto file_it = io_mgr_->closed_files_.find(key);
                if (file_it == io_mgr_->closed_files_.end())
                {
                    ++it;
                    continue;
                }

                CachedFile *file = &file_it->second;
                if (io_mgr_->IsEvictingKey(*file->key_))
                {
                    ++it;
                    continue;
                }

                FileKey key_copy = *file->key_;
                if (!io_mgr_->StartEvictingKey(key_copy))
                {
                    ++it;
                    continue;
                }
                it = io_mgr_->pending_gc_cleanup_.erase(it);
                made_progress = true;

                UnlinkReq &req = unlink_reqs[req_count++];
                req.file_ = file;
                req.path_ = file->key_->tbl_id_.ToString();
                req.path_ /= file->key_->filename_;

                io_uring_sqe *sqe =
                    io_mgr_->GetSQE(UserDataType::BaseReq, &req);
                int root_fd = io_mgr_->GetRootFD(file->key_->tbl_id_).first;
                DLOG(INFO) << "FileCleaner GC:" << req.path_;
                io_uring_prep_unlinkat(sqe, root_fd, req.path_.c_str(), 0);
            }
        }

        if (req_count < batch_size && needs_pressure_eviction)
        {
            for (CachedFile *file = io_mgr_->lru_file_tail_.prev_;
                 file != &io_mgr_->lru_file_head_;
                 file = file->prev_)
            {
                if (req_count == batch_size)
                {
                    // For pointer stability, we can not reallocate this vector.
                    break;
                }
                if (io_mgr_->pending_gc_cleanup_.contains(*file->key_) ||
                    io_mgr_->IsEvictingKey(*file->key_))
                {
                    continue;
                }

                // Set evicting to block task that try to open it.
                FileKey key_copy = *file->key_;
                if (!io_mgr_->StartEvictingKey(key_copy))
                {
                    continue;
                }

                UnlinkReq &req = unlink_reqs[req_count++];
                req.file_ = file;
                req.path_ = file->key_->tbl_id_.ToString();
                req.path_ /= file->key_->filename_;

                io_uring_sqe *sqe =
                    io_mgr_->GetSQE(UserDataType::BaseReq, &req);
                int root_fd =
                    io_mgr_->GetRootFD(req.file_->key_->tbl_id_).first;
                DLOG(INFO) << "FileCleaner eviction:" << req.path_;
                io_uring_prep_unlinkat(sqe, root_fd, req.path_.c_str(), 0);
            }
        }

        if (req_count == 0)
        {
            if (!made_progress)
            {
                status_ = TaskStatus::Idle;
                Yield();
                if (killed_)
                {
                    break;
                }
                status_ = TaskStatus::Ongoing;
            }
            continue;
        }

        WaitIo();

        for (uint16_t i = 0; i < req_count; i++)
        {
            const UnlinkReq &req = unlink_reqs[i];
            CachedFile *file = req.file_;
            const FileKey key_copy = *file->key_;
            if (req.res_ < 0 && req.res_ != -ENOENT)
            {
                LOG(ERROR) << "unlink file failed: " << req.path_ << " : "
                           << strerror(-req.res_);
                io_mgr_->FinishEvictingKey(key_copy);
                continue;
            }

            size_t file_size = io_mgr_->EstimateFileSize(file->key_->filename_);
            if (file_size <= io_mgr_->used_local_space_)
            {
                io_mgr_->used_local_space_ -= file_size;
            }
            else
            {
                io_mgr_->used_local_space_ = 0;
            }
            touched_partitions.insert(file->key_->tbl_id_);
            file->Deque();
            io_mgr_->DecrementClosedFileCount(file->key_->tbl_id_);
            io_mgr_->closed_files_.erase(key_copy);
            io_mgr_->FinishEvictingKey(key_copy);

            requesting_.WakeOne();
        }

        for (const TableIdent &tbl_id : touched_partitions)
        {
            KvError cleanup_err = io_mgr_->TryCleanupLocalPartitionDir(tbl_id);
            if (cleanup_err != KvError::NoError)
            {
                LOG(WARNING)
                    << "Failed to clean local partition directory for table "
                    << tbl_id
                    << " after file cleanup: " << ErrorString(cleanup_err);
            }
        }
        DLOG(INFO) << "file cleaner send " << req_count << " unlink requests";
    }
}

void CloudStoreMgr::FileCleaner::Shutdown()
{
    // Wake up file cleaner to stop it.
    assert(status_ == TaskStatus::Idle);
    if (killed_)
        return;
    killed_ = true;
    shard->running_ = this;
    coro_ = coro_.resume();
}

MemStoreMgr::MemStoreMgr(const KvOptions *opts) : AsyncIoManager(opts) {};

KvError MemStoreMgr::Init(Shard *shard)
{
    return KvError::NoError;
}

std::pair<Page, KvError> MemStoreMgr::ReadPage(const TableIdent &tbl_id,
                                               FilePageId fp_id,
                                               Page page)
{
    auto it = store_.find(tbl_id);
    if (it == store_.end())
    {
        return {std::move(page), KvError::NotFound};
    }

    Partition &part = it->second;
    if (fp_id >= part.pages.size())
    {
        return {std::move(page), KvError::NotFound};
    }
    memcpy(page.Ptr(), part.pages[fp_id].get(), options_->data_page_size);
    return {std::move(page), KvError::NoError};
}

KvError MemStoreMgr::ReadPages(const TableIdent &tbl_id,
                               std::span<FilePageId> page_ids,
                               std::vector<Page> &pages)
{
    pages.clear();
    pages.reserve(page_ids.size());
    for (FilePageId fp_id : page_ids)
    {
        auto [page, err] = ReadPage(tbl_id, fp_id, Page(true));
        CHECK_KV_ERR(err);
        pages.push_back(std::move(page));
    }
    return KvError::NoError;
}

std::pair<ManifestFilePtr, KvError> MemStoreMgr::GetManifest(
    const TableIdent &tbl_ident)
{
    auto it = store_.find(tbl_ident);
    if (it == store_.end())
    {
        return {nullptr, KvError::NotFound};
    }
    return {std::make_unique<Manifest>(it->second.wal), KvError::NoError};
}

KvError MemStoreMgr::WritePage(const TableIdent &tbl_id,
                               VarPage page,
                               FilePageId file_page_id)
{
    auto it = store_.find(tbl_id);
    if (it == store_.end())
    {
        auto [it1, _] = store_.try_emplace(tbl_id);
        it = it1;
    }
    Partition &part = it->second;

    char *dst;
    if (file_page_id >= part.pages.size())
    {
        assert(file_page_id == part.pages.size());
        part.pages.emplace_back(
            std::make_unique<char[]>(options_->data_page_size));
        dst = part.pages.back().get();
    }
    else
    {
        dst = part.pages[file_page_id].get();
    }
    char *ptr = VarPagePtr(page);
    memcpy(dst, ptr, options_->data_page_size);

    static_cast<WriteTask *>(ThdTask())->WritePageCallback(std::move(page),
                                                           KvError::NoError);
    return KvError::NoError;
}

KvError MemStoreMgr::SyncData(const TableIdent &tbl_id)
{
    return KvError::NoError;
}

KvError MemStoreMgr::AbortWrite(const TableIdent &tbl_id)
{
    return KvError::NoError;
}

KvError MemStoreMgr::CleanManifest(const TableIdent &tbl_id)
{
    return KvError::NoError;
}

KvError MemStoreMgr::AppendManifest(const TableIdent &tbl_id,
                                    std::string_view log,
                                    uint64_t manifest_size)
{
    auto it = store_.find(tbl_id);
    assert(it != store_.end());
    Partition &part = it->second;
    if (manifest_size < part.wal.size())
    {
        part.wal.resize(manifest_size);
    }
    part.wal.append(log);
    return KvError::NoError;
}

KvError MemStoreMgr::SwitchManifest(const TableIdent &tbl_id,
                                    std::string_view snapshot)
{
    auto it = store_.find(tbl_id);
    if (it == store_.end())
    {
        return KvError::NotFound;
    }
    Partition &part = it->second;
    part.wal.clear();
    part.wal.append(snapshot);
    return KvError::NoError;
}

KvError MemStoreMgr::CreateArchive(const TableIdent &tbl_id,
                                   std::string_view branch_name,
                                   uint64_t term,
                                   std::string_view snapshot,
                                   std::string_view tag)
{
    (void) tbl_id;
    (void) snapshot;
    (void) tag;
    LOG(FATAL) << "not implemented";
    return KvError::InvalidArgs;
}

KvError MemStoreMgr::DeleteArchive(const TableIdent &tbl_id,
                                   std::string_view branch_name,
                                   uint64_t term,
                                   std::string_view tag)
{
    (void) tbl_id;
    (void) branch_name;
    (void) term;
    (void) tag;
    LOG(FATAL) << "not implemented";
    return KvError::InvalidArgs;
}

KvError MemStoreMgr::WriteBranchManifest(const TableIdent &tbl_id,
                                         std::string_view branch_name,
                                         uint64_t term,
                                         std::string_view snapshot)
{
    std::lock_guard lock(manifest_mutex_);
    std::string key = BranchManifestFileName(branch_name, term);
    manifests_[tbl_id][key] = std::string(snapshot);
    return KvError::NoError;
}

KvError MemStoreMgr::DeleteBranchFiles(const TableIdent &tbl_id,
                                       std::string_view branch_name,
                                       uint64_t /* term (unused) */)
{
    std::lock_guard lock(manifest_mutex_);

    // MemStoreMgr does not persist term files, so we only know about term 0.
    // Erase the manifest entry for term 0 (the only term used in mem-store
    // tests).
    auto &tbl_manifests = manifests_[tbl_id];
    tbl_manifests.erase(BranchManifestFileName(branch_name, 0));

    return KvError::NoError;
}

KvError MemStoreMgr::Manifest::Read(char *dst, size_t n)
{
    if (content_.length() < n)
    {
        return KvError::EndOfFile;
    }
    memcpy(dst, content_.data(), n);
    content_ = content_.substr(n);
    return KvError::NoError;
}

KvError MemStoreMgr::Manifest::SkipPadding(size_t n)
{
    if (content_.length() < n)
    {
        return KvError::EndOfFile;
    }
    content_.remove_prefix(n);
    return KvError::NoError;
}

KvError CloudStoreMgr::WriteFile(const TableIdent &tbl_id,
                                 std::string_view filename,
                                 const DirectIoBuffer &buffer,
                                 uint64_t flags,
                                 uint64_t offset)
{
    auto [dir_fd, dir_err] =
        OpenOrCreateFD(tbl_id, LruFD::kDirectory, false, true, "", 0);
    if (dir_err != KvError::NoError)
    {
        return dir_err;
    }

    std::string filename_str(filename);
    int fd = OpenAt(dir_fd.FdPair(), filename_str.c_str(), flags, 0644, false);
    if (fd < 0)
    {
        LOG(ERROR) << "failed to open file for write: " << tbl_id << '/'
                   << filename_str << ": " << strerror(-fd);
        return ToKvError(fd);
    }
    FdIdx file_fd{fd, false};

    KvError status = KvError::NoError;
    const size_t padded_size = buffer.padded_size();
    [[maybe_unused]] const size_t logical_size = buffer.size();
    [[maybe_unused]] const size_t alignment = buffer.alignment();
    const char *write_ptr = buffer.data();

    assert(write_ptr != nullptr);
    assert(padded_size > 0);
    assert((reinterpret_cast<uintptr_t>(write_ptr) & (alignment - 1)) == 0);
    assert(logical_size == padded_size);
    assert(offset <= padded_size);
    while (offset < padded_size)
    {
        size_t to_write = padded_size - offset;
        int wres = Write(file_fd, write_ptr + offset, to_write, offset);
        if (wres < 0)
        {
            status = ToKvError(wres);
            break;
        }
        if (wres == 0)
        {
            status = KvError::IoFail;
            break;
        }
        offset += static_cast<size_t>(wres);
    }

    int close_res = Close(fd);
    if (close_res < 0 && status == KvError::NoError)
    {
        status = ToKvError(close_res);
    }
    return status;
}
}  // namespace eloqstore
