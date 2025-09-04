#include "async_io_manager.h"

#include <dirent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <linux/openat2.h>

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "eloq_store.h"
#include "error.h"
#include "kill_point.h"
#include "kv_options.h"
#include "object_store.h"
#include "read_task.h"
#include "shard.h"
#include "task.h"
#include "write_task.h"

namespace eloqstore
{
namespace fs = std::filesystem;

char *VarPagePtr(const VarPage &page)
{
    char *ptr = nullptr;
    switch (VarPageType(page.index()))
    {
    case VarPageType::MemIndexPage:
        ptr = std::get<MemIndexPage *>(page)->PagePtr();
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

std::unique_ptr<AsyncIoManager> AsyncIoManager::Instance(const EloqStore *store,
                                                         uint32_t fd_limit)
{
    const KvOptions *opts = &store->Options();
    if (opts->store_path.empty())
    {
        return std::make_unique<MemStoreMgr>(opts);
    }
    else if (opts->cloud_store_path.empty())
    {
        return std::make_unique<IouringMgr>(opts, fd_limit);
    }
    else
    {
        ObjectStore *obj_store = store->obj_store_.get();
        return std::make_unique<CloudStoreMgr>(opts, fd_limit, obj_store);
    }
}

bool AsyncIoManager::IsIdle()
{
    return true;
}

IouringMgr::IouringMgr(const KvOptions *opts, uint32_t fd_limit)
    : AsyncIoManager(opts), fd_limit_(fd_limit)
{
    lru_fd_head_.next_ = &lru_fd_tail_;
    lru_fd_tail_.prev_ = &lru_fd_head_;

    if (!options_->data_append_mode)
    {
        uint32_t pool_size = options_->max_inflight_write;
        write_req_pool_ = std::make_unique<WriteReqPool>(pool_size);
    }
}

IouringMgr::~IouringMgr()
{
    io_uring_unregister_files(&ring_);
    for (auto &[_, tbl] : tables_)
    {
        for (auto &[_, fd] : tbl.fds_)
        {
            close(fd.fd_);
        }
    }

    io_uring_free_buf_ring(
        &ring_, buf_ring_, options_->buf_ring_size, buf_group_);

    io_uring_queue_exit(&ring_);
}

KvError IouringMgr::Init(Shard *shard)
{
    io_uring_params params = {};
    const uint32_t sq_size = options_->io_queue_size;
    int ret = io_uring_queue_init_params(sq_size, &ring_, &params);
    if (ret < 0)
    {
        LOG(ERROR) << "failed to initialize io queue: " << ret;
        return KvError::IoFail;
    }

    if (fd_limit_ > 0)
    {
        ret = io_uring_register_files_sparse(&ring_, fd_limit_);
        if (ret < 0)
        {
            LOG(ERROR) << "failed to reserve register file slots: " << ret;
            io_uring_queue_exit(&ring_);
            return KvError::OpenFileLimit;
        }
        free_reg_slots_.reserve(fd_limit_);
    }

    uint16_t num_bufs = options_->buf_ring_size;
    assert(num_bufs);
    uint16_t buf_size = options_->data_page_size;
    buf_ring_ = io_uring_setup_buf_ring(&ring_, num_bufs, buf_group_, 0, &ret);
    if (buf_ring_ == nullptr)
    {
        LOG(ERROR) << "failed to initialize buffer ring: " << ret;
        io_uring_unregister_files(&ring_);
        io_uring_queue_exit(&ring_);
        return KvError::OutOfMem;
    }
    int mask = io_uring_buf_ring_mask(num_bufs);
    bufs_pool_.reserve(num_bufs);
    for (uint16_t i = 0; i < num_bufs; i++)
    {
        Page page(shard->PagePool()->Allocate());
        io_uring_buf_ring_add(buf_ring_, page.Ptr(), buf_size, i, mask, i);
        bufs_pool_.emplace_back(std::move(page));
    }
    io_uring_buf_ring_advance(buf_ring_, num_bufs);

    return KvError::NoError;
}

std::pair<Page, KvError> IouringMgr::ReadPage(const TableIdent &tbl_id,
                                              FilePageId fp_id,
                                              Page page)
{
    auto [file_id, offset] = ConvFilePageId(fp_id);
    auto [fd_ref, err] = OpenFD(tbl_id, file_id);
    if (err != KvError::NoError)
    {
        return {std::move(page), err};
    }

    auto read_page = [this](FdIdx fd, size_t offset, Page &result) -> KvError
    {
        int res;
        do
        {
            io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
            if (fd.second)
            {
                sqe->flags |= IOSQE_FIXED_FILE;
            }
            sqe->buf_group = buf_group_;
            sqe->flags |= IOSQE_BUFFER_SELECT;
            io_uring_prep_read(sqe, fd.first, NULL, 0, offset);
            res = ThdTask()->WaitIoResult();
            if (ThdTask()->io_flags_ & IORING_CQE_F_BUFFER)
            {
                uint16_t buf_id =
                    ThdTask()->io_flags_ >> IORING_CQE_BUFFER_SHIFT;
                result = SwapPage(std::move(result), buf_id);
            }
            if (res == 0)
            {
                LOG(ERROR) << "read page failed, reach end of file";
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

    struct ReadReq : public BaseReq
    {
        ReadReq() = default;
        ReadReq(KvTask *task, LruFD::Ref fd, uint32_t offset)
            : BaseReq(task),
              fd_ref_(std::move(fd)),
              offset_(offset),
              page_(true){};

        LruFD::Ref fd_ref_;
        uint32_t offset_;
        Page page_{false};
        bool done_{false};
    };

    // ReadReq is a temporary object, so we allocate it on stack.
    std::array<ReadReq, max_read_pages_batch> reqs_buf;
    std::span<ReadReq> reqs(reqs_buf.data(), page_ids.size());

    // Prepare requests.
    for (uint8_t i = 0; FilePageId fp_id : page_ids)
    {
        auto [file_id, offset] = ConvFilePageId(fp_id);
        auto [fd_ref, err] = OpenFD(tbl_id, file_id);
        if (err != KvError::NoError)
        {
            return err;
        }
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
        sqe->buf_group = buf_group_;
        sqe->flags |= IOSQE_BUFFER_SELECT;
        io_uring_prep_read(sqe, fd, NULL, 0, req->offset_);
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
            if (req.flags_ & IORING_CQE_F_BUFFER)
            {
                uint16_t buf_id = req.flags_ >> IORING_CQE_BUFFER_SHIFT;
                req.page_ = SwapPage(std::move(req.page_), buf_id);
            }

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
    auto [fd, err] = OpenFD(tbl_id, LruFD::kManifest, true);
    if (err != KvError::NoError)
    {
        return {nullptr, err};
    }
    struct statx result = {};
    int res = Statx(fd.Get()->fd_, "", &result);
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
    auto [fd_ref, err] = OpenOrCreateFD(tbl_id, file_id);
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
    io_uring_prep_write(sqe, fd, ptr, options_->data_page_size, offset);
    return KvError::NoError;
}

KvError IouringMgr::WritePages(const TableIdent &tbl_id,
                               std::span<VarPage> pages,
                               FilePageId first_fp_id)
{
    auto [file_id, offset] = ConvFilePageId(first_fp_id);
    auto [fd_ref, err] = OpenOrCreateFD(tbl_id, file_id);
    CHECK_KV_ERR(err);
    fd_ref.Get()->dirty_ = true;

    auto writev = [this, &fd_ref](std::span<VarPage> pages,
                                  uint32_t offset,
                                  std::span<iovec> iov)
    {
        auto [fd, registered] = fd_ref.FdPair();
        io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
        if (registered)
        {
            sqe->flags |= IOSQE_FIXED_FILE;
        }

        size_t num_pages = pages.size();
        for (size_t i = 0; i < num_pages; i++)
        {
            iov[i].iov_base = VarPagePtr(pages[i]);
            iov[i].iov_len = options_->data_page_size;
        }
        io_uring_prep_writev(sqe, fd, iov.data(), num_pages, offset);

        int ret = ThdTask()->WaitIoResult();
        if (ret < 0)
        {
            return ToKvError(ret);
        }
        if (ret < (num_pages * options_->data_page_size))
        {
            return KvError::TryAgain;
        }
        return KvError::NoError;
    };

    TEST_KILL_POINT_WEIGHT("WritePages", 100)
    size_t num_pages = pages.size();
    if (num_pages <= 256)
    {
        // Allocate iovec on stack (4KB).
        std::array<iovec, 256> iov;
        return writev(pages, offset, iov);
    }
    else
    {
        // Allocate iovec on heap when required space exceed 4KB.
        std::vector<iovec> iov;
        iov.resize(num_pages);
        return writev(pages, offset, iov);
    }
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

void IouringMgr::CleanTable(const TableIdent &tbl_id)
{
    // TODO: io_uring_prep_unlinkat
    assert(false);
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
    const uint16_t n_disks = eloq_store->root_fds_.size();
    int root_fd = eloq_store->root_fds_[tbl_id.DiskIndex(n_disks)];
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
    LruFD::Ref fd_ref(&it_fd->second, this);
    while (fd_ref.Get()->fd_ < 0)
    {
        switch (fd_ref.Get()->fd_)
        {
        case LruFD::FdEmpty:
            return nullptr;
        case LruFD::FdLocked:
            fd_ref.Get()->waiting_.Wait(ThdTask());
            break;
        default:
            assert(false);
        }
    }
    return fd_ref;
}

IouringMgr::LruFD::Ref IouringMgr::GetFDSlot(const TableIdent &tbl_id,
                                             FileId file_id)
{
    auto it_tbl = tables_.find(tbl_id);
    if (it_tbl == tables_.end())
    {
        auto [it, _] = tables_.try_emplace(tbl_id);
        it->second.tbl_id_ = &it->first;
        it_tbl = it;
    }
    PartitionFiles *tbl = &it_tbl->second;

    auto it_fd = tbl->fds_.find(file_id);
    if (it_fd == tbl->fds_.end())
    {
        auto [it, _] = tbl->fds_.try_emplace(file_id, tbl, file_id);
        it_fd = it;
    }
    return {&it_fd->second, this};
}

std::pair<IouringMgr::LruFD::Ref, KvError> IouringMgr::OpenFD(
    const TableIdent &tbl_id, FileId file_id, bool direct)
{
    return OpenOrCreateFD(tbl_id, file_id, direct, false);
}

std::pair<IouringMgr::LruFD::Ref, KvError> IouringMgr::OpenOrCreateFD(
    const TableIdent &tbl_id, FileId file_id, bool direct, bool create)
{
    LruFD::Ref lru_fd = GetFDSlot(tbl_id, file_id);
    assert(lru_fd != nullptr);

    while (lru_fd.Get()->fd_ < 0)
    {
        switch (lru_fd.Get()->fd_)
        {
        case LruFD::FdEmpty:
        {
            // Note: Never yield between compare FdEmpty and set FdLocked
            lru_fd.Get()->fd_ = LruFD::FdLocked;

            int fd;
            KvError error = KvError::NoError;
            if (file_id == LruFD::kDirectory)
            {
                FdIdx root_fd = GetRootFD(tbl_id);
                std::string dirname = tbl_id.ToString();
                fd = OpenAt(root_fd, dirname.c_str(), oflags_dir);
                if (fd == -ENOENT && create)
                {
                    fd = MakeDir(root_fd, dirname.c_str());
                }
            }
            else
            {
                fd = OpenFile(tbl_id, file_id, direct);
                if (fd == -ENOENT && create)
                {
                    // This must be data file because manifest should always be
                    // created by call WriteSnapshot.
                    assert(file_id <= LruFD::kMaxDataFile);
                    auto [dfd_ref, err] =
                        OpenOrCreateFD(tbl_id, LruFD::kDirectory);
                    error = err;
                    if (dfd_ref != nullptr)
                    {
                        TEST_KILL_POINT_WEIGHT("OpenOrCreateFD:CreateFile", 100)
                        fd = CreateFile(std::move(dfd_ref), file_id);
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
                if (file_id == LruFD::kManifest && error == KvError::NotFound)
                {
                    // Manifest not found, this is normal so don't log it.
                }
                else
                {
                    LOG(ERROR) << "open failed " << tbl_id << " file id "
                               << file_id << " : " << ErrorString(error);
                }
                lru_fd.Get()->fd_ = LruFD::FdEmpty;
                lru_fd.Get()->waiting_.WakeAll();
                return {nullptr, error};
            }

            if (file_id != LruFD::kDirectory)
            {
                lru_fd.Get()->reg_idx_ = RegisterFile(fd);
            }

            lru_fd.Get()->fd_ = fd;
            // notify all waiting tasks success
            lru_fd.Get()->waiting_.WakeAll();
            break;
        }
        case LruFD::FdLocked:
        {
            lru_fd.Get()->waiting_.Wait(ThdTask());
            break;
        }
        default:
            LOG(FATAL) << "Unexpected fd status " << lru_fd.Get()->fd_;
        }
    }

    return {std::move(lru_fd), KvError::NoError};
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
    if (prepared_sqe_ == 0)
    {
        return;
    }
    int ret = io_uring_submit(&ring_);
    if (ret < 0)
    {
        LOG(ERROR) << "iouring submit failed " << ret;
    }
    else
    {
        prepared_sqe_ -= ret;
    }
}

void IouringMgr::PollComplete()
{
    io_uring_cqe *cqe;
    unsigned head;
    unsigned cnt = 0;
    io_uring_for_each_cqe(&ring_, head, cqe)
    {
        cnt++;

        auto [ptr, type] = DecodeUserData(cqe->user_data);
        KvTask *task;
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
        default:
            assert(false);
        }
        task->FinishIo();
    }

    io_uring_cq_advance(&ring_, cnt);
    waiting_sqe_.WakeN(cnt);
}

int IouringMgr::MakeDir(FdIdx dir_fd, const char *path)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    if (dir_fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
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
    return OpenAt(dir_fd, path, oflags_dir);
}

int IouringMgr::CreateFile(LruFD::Ref dir_fd, FileId file_id)
{
    assert(file_id <= LruFD::kMaxDataFile);
    uint64_t flags = O_CREAT | O_RDWR | O_DIRECT;
    std::string filename = DataFileName(file_id);
    int fd = OpenAt(dir_fd.FdPair(), filename.c_str(), flags, 0644);
    if (fd >= 0)
    {
        // Multiple data files may be created in one
        // WriteTask. Table partition directory FD will
        // be marked as dirty every time, but only fsync
        // it once when SyncData.
        dir_fd.Get()->dirty_ = true;
        Fallocate({fd, false}, options_->DataFileSize());
    }
    return fd;
}

int IouringMgr::OpenFile(const TableIdent &tbl_id, FileId file_id, bool direct)
{
    uint64_t flags = O_RDWR;
    fs::path path = tbl_id.ToString();
    if (file_id == LruFD::kManifest)
    {
        if (direct)
        {
            flags |= O_DIRECT;
        }
        path.append(FileNameManifest);
    }
    else
    {
        // Data file is always opened with O_DIRECT.
        flags |= O_DIRECT;
        assert(file_id <= LruFD::kMaxDataFile);
        path.append(DataFileName(file_id));
    }
    FdIdx root_fd = GetRootFD(tbl_id);
    return OpenAt(root_fd, path.c_str(), flags);
}

int IouringMgr::OpenAt(FdIdx dir_fd,
                       const char *path,
                       uint64_t flags,
                       uint64_t mode)
{
    lru_fd_count_++;
    EvictFD();

    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    if (dir_fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    open_how how = {.flags = flags, .mode = mode, .resolve = 0};
    io_uring_prep_openat2(sqe, dir_fd.first, path, &how);
    int fd = ThdTask()->WaitIoResult();

    if (fd < 0)
    {
        lru_fd_count_--;
    }
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

Page IouringMgr::SwapPage(Page page, uint16_t buf_id)
{
    assert(buf_id < bufs_pool_.size());
    std::swap(page, bufs_pool_[buf_id]);

    uint16_t buf_size = options_->data_page_size;
    int mask = io_uring_buf_ring_mask(options_->buf_ring_size);
    io_uring_buf_ring_add(
        buf_ring_, bufs_pool_[buf_id].Ptr(), buf_size, buf_id, mask, 0);
    io_uring_buf_ring_advance(buf_ring_, 1);
    return page;
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
    if (res >= 0)
    {
        fd.Get()->dirty_ = false;
    }
    return ToKvError(res);
}

KvError IouringMgr::SyncFiles(const TableIdent &tbl_id,
                              std::span<LruFD::Ref> fds)
{
    struct FsyncReq : BaseReq
    {
        FsyncReq(KvTask *task, LruFD::Ref fd)
            : BaseReq(task), fd_ref_(std::move(fd)){};
        LruFD::Ref fd_ref_;
    };

    // Fsync all dirty files/directory.
    std::vector<FsyncReq> reqs;
    reqs.reserve(fds.size());
    for (LruFD::Ref fd_ref : fds)
    {
        // FsyncReq elements have pointer stability, because we have reserved
        // enough space for this vector so that it will never reallocate.
        const FsyncReq &req = reqs.emplace_back(ThdTask(), std::move(fd_ref));
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
        else
        {
            req.fd_ref_.Get()->dirty_ = false;
        }
    }
    return err;
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

int IouringMgr::Statx(int fd, const char *path, struct statx *result)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_statx(
        sqe, fd, path, AT_EMPTY_PATH, STATX_BASIC_STATS, result);
    return ThdTask()->WaitIoResult();
}

int IouringMgr::Rename(FdIdx dir_fd, const char *old_path, const char *new_path)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    if (dir_fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
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

KvError IouringMgr::CloseFile(LruFD::Ref fd_ref)
{
    LruFD *lru_fd = fd_ref.Get();
    assert(lru_fd->waiting_.Empty());
    const int fd = lru_fd->fd_;
    if (fd < 0)
    {
        return KvError::NoError;
    }
    FdIdx fd_idx = lru_fd->FdPair();

    // Make sure no tasks can use this fd during closing.
    lru_fd->fd_ = LruFD::FdLocked;

    if (lru_fd->dirty_)
    {
        if (KvError err = SyncFile(fd_ref); err != KvError::NoError)
        {
            lru_fd->fd_ = fd;
            lru_fd->waiting_.WakeAll();
            return err;
        }
    }

    if (fd_idx.second)
    {
        UnregisterFile(fd_idx.first);
        lru_fd->reg_idx_ = -1;
    }

    if (int res = Close(fd); res < 0)
    {
        lru_fd->fd_ = fd;
        lru_fd->waiting_.WakeAll();
        return ToKvError(res);
    }
    lru_fd->fd_ = LruFD::FdEmpty;
    lru_fd->waiting_.WakeAll();
    return KvError::NoError;
}

bool IouringMgr::EvictFD()
{
    while (lru_fd_count_ > fd_limit_)
    {
        LruFD *lru_fd = lru_fd_tail_.prev_;
        if (lru_fd == &lru_fd_head_)
        {
            return false;
        }
        assert(lru_fd->ref_count_ == 0);
        assert(lru_fd->fd_ >= 0);
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

int IouringMgr::RegisterFile(int fd)
{
    uint32_t idx = AllocRegisterIndex();
    if (idx == UINT32_MAX)
    {
        LOG(WARNING) << "register file slot used up: " << lru_fd_count_;
        return -1;
    }
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_files_update(sqe, &fd, 1, idx);
    int res = ThdTask()->WaitIoResult();
    if (res < 0)
    {
        LOG(ERROR) << "failed to register file " << fd << " at " << idx << ": "
                   << strerror(-res);
        FreeRegisterIndex(idx);
        return -1;
    }
    return idx;
}

int IouringMgr::UnregisterFile(int idx)
{
    int fd = -1;
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, ThdTask());
    io_uring_prep_files_update(sqe, &fd, 1, idx);
    int res = ThdTask()->WaitIoResult();
    if (res < 0)
    {
        LOG(ERROR) << "can't unregister file at " << idx << ": "
                   << strerror(-res);
    }

    FreeRegisterIndex(idx);
    return res;
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
                                   uint64_t manifest_size)
{
    assert(manifest_size > 0);
    auto [fd_ref, err] = OpenFD(tbl_id, LruFD::kManifest);
    CHECK_KV_ERR(err);

    TEST_KILL_POINT_WEIGHT("AppendManifest:Write", 10)
    int res = Write(fd_ref.FdPair(), log.data(), log.size(), manifest_size);
    if (res < 0)
    {
        LOG(ERROR) << "append manifest failed " << tbl_id;
        return ToKvError(res);
    }

    TEST_KILL_POINT_WEIGHT("AppendManifest:Sync", 10)
    return SyncFile(std::move(fd_ref));
}

int IouringMgr::WriteSnapshot(LruFD::Ref dir_fd,
                              std::string_view name,
                              std::string_view content)
{
    std::string tmpfile = std::string(name) + TmpSuffix;
    uint64_t tmp_oflags = O_CREAT | O_TRUNC | O_RDWR;
    int tmp_fd = OpenAt(dir_fd.FdPair(), tmpfile.c_str(), tmp_oflags, 0644);
    if (tmp_fd < 0)
    {
        LOG(ERROR) << "create temporary file failed " << strerror(-tmp_fd);
        return tmp_fd;
    }

    int res = Write({tmp_fd, false}, content.data(), content.size(), 0);
    if (res < 0)
    {
        Close(tmp_fd);
        LOG(ERROR) << "write temporary file failed " << strerror(-res);
        return res;
    }
    TEST_KILL_POINT("AtomicWriteFile:Sync")
    res = Fdatasync({tmp_fd, false});
    if (res < 0)
    {
        Close(tmp_fd);
        LOG(ERROR) << "fsync temporary file failed " << strerror(-res);
        return res;
    }

    // Switch file on disk.
    TEST_KILL_POINT("AtomicWriteFile:Rename")
    res = Rename(dir_fd.FdPair(), tmpfile.c_str(), name.data());
    if (res < 0)
    {
        Close(tmp_fd);
        LOG(ERROR) << "rename temporary file failed " << strerror(-res);
        return res;
    }
    TEST_KILL_POINT("AtomicWriteFile:SyncDir")
    res = Fdatasync(dir_fd.FdPair());
    if (res < 0)
    {
        Close(tmp_fd);
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

    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory);
    CHECK_KV_ERR(err);
    int res = WriteSnapshot(std::move(dir_fd), FileNameManifest, snapshot);
    if (res < 0)
    {
        return ToKvError(res);
    }
    Close(res);
    return KvError::NoError;
}

KvError IouringMgr::CreateArchive(const TableIdent &tbl_id,
                                  std::string_view snapshot,
                                  uint64_t ts)
{
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory);
    CHECK_KV_ERR(err);
    const std::string name = ArchiveName(ts);
    int res = WriteSnapshot(std::move(dir_fd), name, snapshot);
    if (res < 0)
    {
        return ToKvError(res);
    }
    Close(res);
    return KvError::NoError;
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

KvError IouringMgr::Manifest::Read(char *dst, size_t n)
{
    while (n > 0)
    {
        if (buf_offset_ >= buf_end_)
        {
            if (file_offset_ >= file_size_)
            {
                // Reached end of file.
                return KvError::EndOfFile;
            }
            // Read the next page.
            size_t expected =
                std::min(file_size_ - file_offset_, size_t(buf_size));
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
            else if (res < expected)
            {
                // Read less than expected, we need to ensure file_offset_ is
                // aligned for the next retry operation.
                res &= ~(page_align - 1);
            }

            file_offset_ += res;
            buf_end_ = res;
            buf_offset_ = 0;
        }

        size_t bytes = std::min(size_t(buf_end_ - buf_offset_), n);
        memcpy(dst, buf_.get() + buf_offset_, bytes);
        buf_offset_ += bytes;
        dst += bytes;
        n -= bytes;
    }
    return KvError::NoError;
}

IouringMgr::LruFD::LruFD(PartitionFiles *tbl, FileId file_id)
    : tbl_(tbl), file_id_(file_id)
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

IouringMgr::LruFD *IouringMgr::LruFD::DequeNext()
{
    LruFD *target = next_;
    if (target != nullptr)
    {
        next_ = target->next_;
        if (next_ != nullptr)
        {
            next_->prev_ = this;
        }

        target->prev_ = nullptr;
        target->next_ = nullptr;
    }

    return target;
}

IouringMgr::LruFD *IouringMgr::LruFD::DequePrev()
{
    LruFD *target = prev_;
    if (target != nullptr)
    {
        prev_ = target->prev_;
        if (prev_ != nullptr)
        {
            prev_->next_ = this;
        }
        target->prev_ = nullptr;
        target->next_ = nullptr;
    }

    return target;
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
        PartitionFiles *tbl = fd_->tbl_;
        if (fd_->fd_ >= 0)
        {
            io_mgr_->lru_fd_head_.EnqueNext(fd_);
        }
        else
        {
            tbl->fds_.erase(fd_->file_id_);
            if (tbl->fds_.empty())
            {
                io_mgr_->tables_.erase(*tbl->tbl_id_);
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
        page_.emplace<MemIndexPage *>(std::get<MemIndexPage *>(page));
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

CloudStoreMgr::CloudStoreMgr(const KvOptions *opts,
                             uint32_t fd_limit,
                             ObjectStore *obj_store)
    : IouringMgr(opts, fd_limit), file_cleaner_(this), obj_store_(obj_store)
{
    lru_file_head_.next_ = &lru_file_tail_;
    lru_file_tail_.prev_ = &lru_file_head_;
}

void CloudStoreMgr::Start()
{
    file_cleaner_.coro_ = boost::context::callcc(
        [this](continuation &&sink)
        {
            shard->main_ = std::move(sink);
            file_cleaner_.Run();
            return std::move(shard->main_);
        });
}

bool CloudStoreMgr::IsIdle()
{
    return file_cleaner_.status_ == TaskStatus::Idle;
}

void CloudStoreMgr::Stop()
{
    file_cleaner_.Shutdown();
}

void CloudStoreMgr::PollComplete()
{
    IouringMgr::PollComplete();

    std::array<KvTask *, 128> buf;
    size_t n = obj_complete_q_.try_dequeue_bulk(buf.data(), buf.size());
    while (n > 0)
    {
        for (size_t i = 0; i < n; i++)
        {
            buf[i]->Resume();
        }
        if (n == buf.size())
        {
            n = obj_complete_q_.try_dequeue_bulk(buf.data(), buf.size());
        }
        else
        {
            assert(n < buf.size());
            // There is a high likelihood that the queue has been exhausted by
            // the previous dequeue operation and no new entries was added
            // during KvTask::Resume because it is fast.
            break;
        }
    }
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
    FileKey fkey(tbl_id, ToFilename(LruFD::kManifest));
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

    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory);
    CHECK_KV_ERR(err);
    int res = WriteSnapshot(std::move(dir_fd), FileNameManifest, snapshot);
    if (res < 0)
    {
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
    err = UploadFiles(tbl_id, {std::string(FileNameManifest)});
    if (err != KvError::NoError)
    {
        LOG(FATAL) << "can not upload manifest: ", ErrorString(err);
    }

    IouringMgr::Close(res);
    EnqueClosedFile(std::move(fkey));
    return KvError::NoError;
}

KvError CloudStoreMgr::CreateArchive(const TableIdent &tbl_id,
                                     std::string_view snapshot,
                                     uint64_t ts)
{
    auto [dir_fd, err] = OpenFD(tbl_id, LruFD::kDirectory);
    CHECK_KV_ERR(err);
    int res = ReserveCacheSpace(options_->manifest_limit);
    if (res < 0)
    {
        return ToKvError(res);
    }
    const std::string name = ArchiveName(ts);
    res = WriteSnapshot(std::move(dir_fd), name, snapshot);
    if (res < 0)
    {
        return ToKvError(res);
    }
    err = UploadFiles(tbl_id, {name});
    IouringMgr::Close(res);
    used_local_space_ += options_->manifest_limit;
    EnqueClosedFile(FileKey(tbl_id, name));
    return err;
}

void CloudStoreMgr::OnObjectStoreComplete(KvTask *task)
{
    obj_complete_q_.enqueue(task);
}

int CloudStoreMgr::CreateFile(LruFD::Ref dir_fd, FileId file_id)
{
    size_t size = options_->DataFileSize();
    int res = ReserveCacheSpace(size);
    if (res < 0)
    {
        return res;
    }
    res = IouringMgr::CreateFile(std::move(dir_fd), file_id);
    if (res >= 0)
    {
        used_local_space_ += size;
    }
    return res;
}

int CloudStoreMgr::OpenFile(const TableIdent &tbl_id,
                            FileId file_id,
                            bool direct)
{
    FileKey key = FileKey(tbl_id, ToFilename(file_id));
    if (DequeClosedFile(key))
    {
        // Try to open the file cached locally.
        int res = IouringMgr::OpenFile(tbl_id, file_id, direct);
        if (res < 0 && res != -ENOENT)
        {
            EnqueClosedFile(std::move(key));
        }
        return res;
    }

    // File not exists locally, try to download it from cloud.
    size_t size = EstimateFileSize(file_id);
    int res = ReserveCacheSpace(size);
    if (res < 0)
    {
        return res;
    }
    KvError err = DownloadFile(tbl_id, file_id);
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
    res = IouringMgr::OpenFile(tbl_id, file_id, direct);
    if (res < 0 && res != -ENOENT)
    {
        EnqueClosedFile(std::move(key));
    }
    return res;
}

KvError CloudStoreMgr::SyncFile(LruFD::Ref fd)
{
    if (int res = IouringMgr::Fdatasync(fd.FdPair()); res < 0)
    {
        return ToKvError(res);
    }

    FileId file_id = fd.Get()->file_id_;
    if (file_id != LruFD::kDirectory)
    {
        const TableIdent &tbl_id = *fd.Get()->tbl_->tbl_id_;
        KvError err = UploadFiles(tbl_id, {ToFilename(file_id)});
        if (err != KvError::NoError)
        {
            if (file_id == LruFD::kManifest)
            {
                LOG(FATAL) << "can not upload manifest: " << ErrorString(err);
            }
            return err;
        }
    }

    fd.Get()->dirty_ = false;
    return KvError::NoError;
}

KvError CloudStoreMgr::SyncFiles(const TableIdent &tbl_id,
                                 std::span<LruFD::Ref> fds)
{
    KvError err = IouringMgr::SyncFiles(tbl_id, fds);
    CHECK_KV_ERR(err);
    std::vector<std::string> filenames;
    for (LruFD::Ref fd : fds)
    {
        FileId file_id = fd.Get()->file_id_;
        if (file_id != LruFD::kDirectory)
        {
            filenames.emplace_back(ToFilename(file_id));
        }
    }
    return UploadFiles(tbl_id, std::move(filenames));
}

KvError CloudStoreMgr::CloseFile(LruFD::Ref fd)
{
    KvError err = IouringMgr::CloseFile(fd);
    CHECK_KV_ERR(err);
    FileId file_id = fd.Get()->file_id_;
    if (file_id != LruFD::kDirectory)
    {
        const TableIdent *tbl_id = fd.Get()->tbl_->tbl_id_;
        EnqueClosedFile(FileKey(*tbl_id, ToFilename(file_id)));
    }
    return KvError::NoError;
}

std::string CloudStoreMgr::ToFilename(FileId file_id)
{
    if (file_id == LruFD::kManifest)
    {
        return FileNameManifest;
    }
    else
    {
        assert(file_id <= LruFD::kMaxDataFile);
        return DataFileName(file_id);
    }
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
}

bool CloudStoreMgr::DequeClosedFile(const FileKey &key)
{
    auto it = closed_files_.find(key);
    while (it != closed_files_.end() && it->second.evicting_)
    {
        // At most one task can try to open a closed file.
        it->second.waiting_.Wait(ThdTask());
        it = closed_files_.find(key);
    }
    if (it == closed_files_.end())
    {
        return false;
    }
    CachedFile *local_file = &it->second;
    local_file->Deque();
    closed_files_.erase(it);
    return true;
}

void CloudStoreMgr::EnqueClosedFile(FileKey key)
{
    assert(key.tbl_id_.IsValid());
    auto [it, ok] = closed_files_.try_emplace(std::move(key));
    assert(ok);
    it->second.key_ = &it->first;
    lru_file_head_.EnqueNext(&it->second);
}

bool CloudStoreMgr ::HasEvictableFile() const
{
    return lru_file_tail_.prev_ != &lru_file_head_;
}

int CloudStoreMgr::ReserveCacheSpace(size_t size)
{
    while (used_local_space_ + size > options_->local_space_limit)
    {
        if (!HasEvictableFile())
        {
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

KvError CloudStoreMgr::DownloadFile(const TableIdent &tbl_id, FileId file_id)
{
    std::string filename = ToFilename(file_id);
    ObjectStore::DownloadTask obj_task(this, ThdTask(), &tbl_id, filename);
    obj_store_->submit_q_.enqueue(&obj_task);
    ThdTask()->status_ = TaskStatus::Blocked;
    ThdTask()->Yield();
    return obj_task.error_;
}

KvError CloudStoreMgr::UploadFiles(const TableIdent &tbl_id,
                                   std::vector<std::string> filenames)
{
    if (filenames.empty())
    {
        return KvError::NoError;
    }
    ObjectStore::UploadTask obj_task(
        this, ThdTask(), &tbl_id, std::move(filenames));
    obj_store_->submit_q_.enqueue(&obj_task);
    ThdTask()->status_ = TaskStatus::Blocked;
    ThdTask()->Yield();
    return obj_task.error_;
}

TaskType CloudStoreMgr::FileCleaner::Type() const
{
    return TaskType::EvictFile;
}

void CloudStoreMgr::FileCleaner::Run()
{
    killed_ = false;
    const KvOptions *opts = io_mgr_->options_;
    const size_t per_shard_limit = opts->local_space_limit / opts->num_threads;
    const size_t reserve_space =
        opts->reserve_space_ratio == 0
            ? 0
            : double(per_shard_limit) / double(opts->reserve_space_ratio);
    const size_t threshold = per_shard_limit - reserve_space;
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
        if (!io_mgr_->HasEvictableFile() ||
            (io_mgr_->used_local_space_ <= threshold && requesting_.Empty()))
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
        for (CachedFile *file = io_mgr_->lru_file_tail_.prev_;
             file != &io_mgr_->lru_file_head_;
             file = file->prev_)
        {
            if (req_count == batch_size)
            {
                // For pointer stability, we can not reallocate this vector.
                break;
            }

            // Set evicting to block task that try to open it.
            file->evicting_ = true;

            UnlinkReq &req = unlink_reqs[req_count++];
            req.file_ = file;
            req.path_ = file->key_->tbl_id_.ToString();
            req.path_.append(file->key_->filename_);

            io_uring_sqe *sqe = io_mgr_->GetSQE(UserDataType::BaseReq, &req);
            int root_fd = io_mgr_->GetRootFD(req.file_->key_->tbl_id_).first;
            io_uring_prep_unlinkat(sqe, root_fd, req.path_.c_str(), 0);
        }

        WaitIo();

        for (uint16_t i = 0; i < req_count; i++)
        {
            const UnlinkReq &req = unlink_reqs[i];
            CachedFile *file = req.file_;
            if (req.res_ < 0)
            {
                LOG(ERROR) << "unlink file failed: " << req.path_ << " : "
                           << strerror(-req.res_);
                file->evicting_ = false;
                file->waiting_.Wake();
                continue;
            }

            size_t file_size = io_mgr_->EstimateFileSize(file->key_->filename_);
            io_mgr_->used_local_space_ -= file_size;
            file->Deque();
            file->waiting_.Wake();
            io_mgr_->closed_files_.erase(*file->key_);

            requesting_.WakeOne();
        }
        DLOG(INFO) << "file cleaner send " << req_count << " unlink requests";
    }
}

void CloudStoreMgr::FileCleaner::Shutdown()
{
    // Wake up file cleaner to stop it.
    assert(status_ == TaskStatus::Idle);
    killed_ = true;
    coro_ = coro_.resume();
}

MemStoreMgr::MemStoreMgr(const KvOptions *opts) : AsyncIoManager(opts)
{
}

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

KvError MemStoreMgr::WritePages(const TableIdent &tbl_id,
                                std::span<VarPage> pages,
                                FilePageId first_fp_id)
{
    LOG(FATAL) << "not implemented";
}

KvError MemStoreMgr::SyncData(const TableIdent &tbl_id)
{
    return KvError::NoError;
}

KvError MemStoreMgr::AbortWrite(const TableIdent &tbl_id)
{
    return KvError::NoError;
}

void MemStoreMgr::CleanTable(const TableIdent &tbl_id)
{
    store_.erase(tbl_id);
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
                                   std::string_view snapshot,
                                   uint64_t ts)
{
    LOG(FATAL) << "not implemented";
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

void MemStoreMgr::Manifest::Skip(size_t n)
{
    if (content_.length() < n)
    {
        content_ = {};
    }
    else
    {
        content_ = content_.substr(n);
    }
}

}  // namespace eloqstore