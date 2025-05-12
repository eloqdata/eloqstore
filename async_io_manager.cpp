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
#include "error.h"
#include "kill_point.h"
#include "kv_options.h"
#include "read_task.h"
#include "shard.h"
#include "task.h"
#include "write_task.h"

namespace kvstore
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
        ptr = std::get<Page>(page).get();
        break;
    }
    assert(!((uint64_t) ptr & (page_align - 1)));
    return ptr;
}

std::unique_ptr<AsyncIoManager> AsyncIoManager::New(const KvOptions *opts)
{
    if (opts->db_path.empty())
    {
        return std::make_unique<MemStoreMgr>(opts);
    }
    return std::make_unique<IouringMgr>(opts);
}

IouringMgr::IouringMgr(const KvOptions *opts) : AsyncIoManager(opts)
{
    lru_fd_head_.next_ = &lru_fd_tail_;
    lru_fd_tail_.prev_ = &lru_fd_head_;

    write_reqs_pool_.reserve(options_->max_inflight_write);
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

KvError IouringMgr::Init(int dir_fd)
{
    io_uring_params params = {};
    int ret =
        io_uring_queue_init_params(options_->io_queue_size, &ring_, &params);
    if (ret < 0)
    {
        LOG(ERROR) << "failed to initialize io queue: " << ret;
        return KvError::IoFail;
    }

    ret = io_uring_register_files_sparse(&ring_, options_->fd_limit);
    if (ret < 0)
    {
        LOG(ERROR) << "failed to reserve register file slots: " << ret;
        io_uring_queue_exit(&ring_);
        return KvError::IoFail;
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
        return KvError::IoFail;
    }
    int mask = io_uring_buf_ring_mask(num_bufs);
    bufs_pool_.reserve(num_bufs);
    for (uint16_t i = 0; i < num_bufs; i++)
    {
        auto &page = bufs_pool_.emplace_back(AllocPage(buf_size));
        io_uring_buf_ring_add(buf_ring_, page.get(), buf_size, i, mask, i);
    }
    io_uring_buf_ring_advance(buf_ring_, num_bufs);

    dir_fd_idx_ = {dir_fd, false};
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

    int res;
    do
    {
        auto ret_pair = ReadPage(fd_ref.FdPair(), std::move(page), offset);
        page = std::move(ret_pair.first);
        res = ret_pair.second;
    } while ((res >= 0 && res < options_->data_page_size) ||
             ToKvError(res) == KvError::TryAgain);

    if (res < 0)
    {
        return {std::move(page), ToKvError(res)};
    }

    if (!ValidatePageChecksum(page.get(), options_->data_page_size))
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
        reqs[i] = ReadReq(thd_task, std::move(fd_ref), offset);
        i++;
    }

    // Send requests.
    while (true)
    {
        bool all_finished = true;
        for (ReadReq &req : reqs)
        {
            int res = req.io_res_;
            KvError err = ToKvError(res);
            if ((res >= 0 && res < options_->data_page_size) ||
                err == KvError::TryAgain)
            {
                ReadPage(&req);
                all_finished = false;
            }
            else if (err != KvError::NoError)
            {
                // Wait for all of the inflight io requests to complete to avoid
                // PollComplete access invalid ReadReq* on this function stack
                // after returned.
                thd_task->WaitAsynIo();
                return err;
            }
        }
        if (all_finished)
        {
            break;
        }
        // Retry until all requests are completed.
        thd_task->WaitAsynIo();
    }

    // Validate result pages.
    for (ReadReq &req : reqs)
    {
        if (!ValidatePageChecksum(req.page_.get(), options_->data_page_size))
        {
            FileId file_id = req.fd_ref_.Get()->file_id_;
            LOG(ERROR) << "corrupted " << tbl_id << " file " << file_id;
            return KvError::Corrupted;
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
    auto [fd, err] = OpenFD(tbl_id, LruFD::kManifest);
    if (err != KvError::NoError)
    {
        return {nullptr, err};
    }
    return {std::make_unique<Manifest>(this, std::move(fd)), KvError::NoError};
}

KvError IouringMgr::WritePage(const TableIdent &tbl_id,
                              VarPage page,
                              FilePageId file_page_id)
{
    TEST_KILL_POINT("WritePage:0")
    auto [file_id, offset] = ConvFilePageId(file_page_id);
    auto [fd_ref, err] = OpenOrCreateFD(tbl_id, file_id);
    CHECK_KV_ERR(err);
    fd_ref.Get()->dirty_ = true;
    TEST_KILL_POINT("WritePage:1")

    auto [fd, registered] = fd_ref.FdPair();
    WriteReq *req = AllocWriteReq(std::move(fd_ref), std::move(page));
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

    auto [fd, registered] = fd_ref.FdPair();
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (registered)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }

    size_t num_pages = pages.size();
    assert(num_pages < max_write_pages_batch);
    std::array<iovec, max_write_pages_batch> iov;
    for (size_t i = 0; i < num_pages; i++)
    {
        iov[i].iov_base = VarPagePtr(pages[i]);
        iov[i].iov_len = options_->data_page_size;
    }
    io_uring_prep_writev(sqe, fd, iov.data(), num_pages, offset);

    int ret = thd_task->WaitSyncIo();
    if (ret < 0)
    {
        return ToKvError(ret);
    }
    if (ret < (num_pages * Options()->data_page_size))
    {
        return KvError::TryAgain;
    }
    return KvError::NoError;
}

KvError IouringMgr::SyncData(const TableIdent &tbl_id)
{
    auto it_tbl = tables_.find(tbl_id);
    if (it_tbl == tables_.end())
    {
        LOG(WARNING) << "sync table partition not found " << tbl_id;
        return KvError::NoError;
    }

    // Scan all dirty files/directory.
    std::vector<FileId> dirty_files;
    dirty_files.reserve(32);
    for (auto &[file_id, fd] : it_tbl->second.fds_)
    {
        if (fd.dirty_)
        {
            dirty_files.emplace_back(file_id);
        }
    }

    // Fsync all dirty files/directory.
    std::vector<FsyncReq> reqs;
    reqs.reserve(dirty_files.size());
    for (FileId file_id : dirty_files)
    {
        LruFD::Ref fd_ref = GetOpenedFD(tbl_id, file_id);
        if (fd_ref == nullptr)
        {
            continue;
        }
        // reqs vector has reserved enough space, so it will never reallocate.
        // FsyncReq element has pointer stability.
        const FsyncReq &req = reqs.emplace_back(thd_task, std::move(fd_ref));
        auto [fd, registered] = req.fd_ref_.FdPair();
        io_uring_sqe *sqe = GetSQE(UserDataType::FsyncReq, &req);
        if (registered)
        {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
    }
    thd_task->WaitAsynIo();

    // Check results.
    KvError err = KvError::NoError;
    for (const FsyncReq &req : reqs)
    {
        if (req.io_res_ < 0)
        {
            err = ToKvError(req.io_res_);
            LOG(ERROR) << "fsync file failed " << tbl_id << '@'
                       << req.fd_ref_.Get()->file_id_ << " : " << req.io_res_;
        }
        else
        {
            req.fd_ref_.Get()->dirty_ = false;
        }
    }
    return err;
}

KvError IouringMgr::AbortWrite(const TableIdent &tbl_id)
{
    // Wait all WriteReq finished to avoid PollComplete access this WriteTask
    // from WriteReq.task_ after aborted.
    thd_task->WaitAsynIo();

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
    auto it = tables_.find(tbl_id);
    assert(it != tables_.end());
    PartitionFiles &tp = it->second;
    io_uring_sqe *sqe;
    // TODO(zhanghao): remove all directory entries
    sqe = GetSQE(UserDataType::KvTask, thd_task);
    std::string path = tbl_id.ToString();
    io_uring_prep_unlinkat(sqe, dir_fd_idx_.first, path.c_str(), AT_REMOVEDIR);
    if (dir_fd_idx_.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    thd_task->WaitAsynIo();
    tables_.erase(it);
}

KvError IouringMgr::ToKvError(int err_no)
{
    if (err_no >= 0)
    {
        return KvError::NoError;
    }
    switch (err_no)
    {
    case -ENOENT:
        return KvError::NotFound;
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
            fd_ref.Get()->loading_.push_back(thd_task);
            thd_task->status_ = TaskStatus::Blocked;
            thd_task->Yield();
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
    auto get_tbl_partition = [this](const TableIdent &tbl_id)
    {
        auto it_tbl = tables_.find(tbl_id);
        if (it_tbl == tables_.end())
        {
            auto [it, _] = tables_.try_emplace(tbl_id);
            it->second.tbl_id_ = &it->first;
            it_tbl = it;
        }
        return &it_tbl->second;
    };

    PartitionFiles *tbl = get_tbl_partition(tbl_id);

    auto it_fd = tbl->fds_.find(file_id);
    if (it_fd == tbl->fds_.end())
    {
        if (lru_fd_count_ >= options_->fd_limit)
        {
            if (!EvictFD())
            {
                return nullptr;
            }
            // EvictFD will give up the cpu and resume, so tbl has a
            // chance to become invalid.
            tbl = get_tbl_partition(tbl_id);
        }

        lru_fd_count_++;
        auto [it, _] = tbl->fds_.try_emplace(file_id, tbl, file_id);
        it_fd = it;
    }
    return {&it_fd->second, this};
}

std::pair<IouringMgr::LruFD::Ref, KvError> IouringMgr::OpenFD(
    const TableIdent &tbl_id, FileId file_id)
{
    return OpenOrCreateFD(tbl_id, file_id, false);
}

std::pair<IouringMgr::LruFD::Ref, KvError> IouringMgr::OpenOrCreateFD(
    const TableIdent &tbl_id, FileId file_id, bool create)
{
    LruFD::Ref lru_fd = GetFDSlot(tbl_id, file_id);
    if (lru_fd == nullptr)
    {
        return {nullptr, KvError::OpenFileLimit};
    }

    while (lru_fd.Get()->fd_ < 0)
    {
        switch (lru_fd.Get()->fd_)
        {
        case LruFD::FdEmpty:
        {
            lru_fd.Get()->fd_ = LruFD::FdLocked;

            int fd;
            KvError error = KvError::NoError;
            if (file_id == LruFD::kDirectory)
            {
                std::string dirname = tbl_id.ToString();
                fd = OpenAt(dir_fd_idx_, dirname.c_str(), oflags_dir);
                if (fd == -ENOENT && create)
                {
                    fd = CreateDir(dir_fd_idx_, dirname.c_str());
                }
            }
            else if (file_id == LruFD::kManifest || file_id == LruFD::kTmpFile)
            {
                const char *filename = file_id == LruFD::kManifest
                                           ? FileNameManifest
                                           : FileNameTmpfile;
                fs::path path = tbl_id.ToString();
                path.append(filename);
                fd = OpenAt(dir_fd_idx_, path.c_str(), O_RDWR);
                if (fd == -ENOENT && create)
                {
                    auto [dfd_ref, err] =
                        OpenOrCreateFD(tbl_id, LruFD::kDirectory);
                    error = err;
                    if (dfd_ref != nullptr)
                    {
                        fd = OpenAt(dfd_ref.FdPair(),
                                    filename,
                                    O_CREAT | O_TRUNC | O_RDWR,
                                    0644);
                        assert(file_id == LruFD::kTmpFile);
                        // AtomicWriteFile will fsync the directory after
                        // rename.
                    }
                }
            }
            else
            {
                assert(file_id <= LruFD::kMaxDataFile);
                std::string filename = DataFileName(file_id);
                fs::path path = tbl_id.ToString();
                path.append(filename);
                fd = OpenAt(dir_fd_idx_, path.c_str(), O_RDWR | O_DIRECT);
                if (fd == -ENOENT && create)
                {
                    auto [dfd_ref, err] =
                        OpenOrCreateFD(tbl_id, LruFD::kDirectory);
                    error = err;
                    assert(dfd_ref != nullptr);
                    if (dfd_ref != nullptr)
                    {
                        fd = OpenAt(dfd_ref.FdPair(),
                                    filename.c_str(),
                                    O_CREAT | O_RDWR | O_DIRECT,
                                    0644);
                        if (fd >= 0)
                        {
                            // Multiple data files may be created in one
                            // WriteTask. Table partition directory FD will be
                            // marked as dirty every time, but only fsync it
                            // once when SyncData.
                            dfd_ref.Get()->dirty_ = true;
                        }
                        assert(fd >= 0);
                    }
                }
            }

            if (fd < 0)
            {
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
                    assert(false);
                }

                lru_fd.Get()->fd_ = LruFD::FdEmpty;
                // notify all waiting tasks failed
                for (KvTask *task : lru_fd.Get()->loading_)
                {
                    task->io_res_ = fd;
                    task->Resume();
                }
                lru_fd.Get()->loading_.clear();
                return {nullptr, error};
            }

            if (file_id != LruFD::kDirectory)
            {
                lru_fd.Get()->reg_idx_ = RegisterFile(fd);
            }
            if (file_id <= LruFD::kMaxDataFile)
            {
                uint64_t file_size = options_->data_page_size
                                     << options_->pages_per_file_shift;
                Fallocate(lru_fd.FdPair(), file_size);
            }

            lru_fd.Get()->fd_ = fd;
            // notify all waiting tasks success
            for (KvTask *task : lru_fd.Get()->loading_)
            {
                task->Resume();
            }
            lru_fd.Get()->loading_.clear();
            break;
        }
        case LruFD::FdLocked:
        {
            lru_fd.Get()->loading_.push_back(thd_task);
            thd_task->status_ = TaskStatus::Blocked;
            thd_task->Yield();
            int res = thd_task->io_res_;
            if (res < 0)
            {
                return {nullptr, ToKvError(res)};
            }
            break;
        }
        default:
            assert(false);
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
    if (not_submitted_sqe_ == 0)
    {
        return;
    }
    TEST_KILL_POINT("Submit:0")
    int ret = io_uring_submit(&ring_);
    TEST_KILL_POINT("Submit:1")
    if (ret < 0)
    {
        LOG(ERROR) << "iouring submit failed " << ret;
    }
    else if (ret > 0)
    {
        not_submitted_sqe_ -= ret;
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

        auto [ptr, typ] = DecodeUserData(cqe->user_data);
        KvTask *task;
        switch (typ)
        {
        case UserDataType::KvTask:
            task = static_cast<KvTask *>(ptr);
            task->io_res_ = cqe->res;
            task->io_flags_ = cqe->flags;
            break;
        case UserDataType::ReadReq:
        {
            ReadReq *req = static_cast<ReadReq *>(ptr);
            req->io_res_ = cqe->res;
            if (cqe->flags & IORING_CQE_F_BUFFER)
            {
                uint16_t buf_id = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
                req->page_ = SwapPage(std::move(req->page_), buf_id);
            }
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
            FreeWriteReq(req);
            break;
        }
        case UserDataType::FsyncReq:
        {
            FsyncReq *req = static_cast<FsyncReq *>(ptr);
            req->io_res_ = cqe->res;
            task = req->task_;
            break;
        }
        default:
            assert(false);
        }
        task->FinishIo(typ == UserDataType::KvTask);
    }

    io_uring_cq_advance(&ring_, cnt);
    waiting_sqe_.WakeN(cnt);
}

int IouringMgr::CreateDir(FdIdx dir_fd, const char *path)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (dir_fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_mkdirat(sqe, dir_fd.first, path, 0775);
    int res = thd_task->WaitSyncIo();
    if (res < 0)
    {
        LOG(ERROR) << "mkdirat " << path << " failed " << res;
        return res;
    }
    res = Fdatasync(dir_fd);
    if (res < 0)
    {
        LOG(ERROR) << "fsync directory failed " << res;
        return res;
    }
    return OpenAt(dir_fd, path, oflags_dir);
}

int IouringMgr::OpenAt(FdIdx dir_fd,
                       const char *path,
                       uint64_t flags,
                       uint64_t mode)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (dir_fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    open_how how = {.flags = flags, .mode = mode, .resolve = 0};
    io_uring_prep_openat2(sqe, dir_fd.first, path, &how);
    return thd_task->WaitSyncIo();
}

int IouringMgr::Read(FdIdx fd, char *dst, size_t n, uint64_t offset)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_read(sqe, fd.first, dst, n, offset);
    return thd_task->WaitSyncIo();
}

std::pair<Page, int> IouringMgr::ReadPage(FdIdx fd, Page page, uint32_t offset)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->buf_group = buf_group_;
    sqe->flags |= IOSQE_BUFFER_SELECT;
    io_uring_prep_read(sqe, fd.first, NULL, 0, offset);
    int ret = thd_task->WaitSyncIo();
    if (thd_task->io_flags_ & IORING_CQE_F_BUFFER)
    {
        uint16_t buf_id = thd_task->io_flags_ >> IORING_CQE_BUFFER_SHIFT;
        page = SwapPage(std::move(page), buf_id);
    }
    return {std::move(page), ret};
}

Page IouringMgr::SwapPage(Page page, uint16_t buf_id)
{
    assert(buf_id < bufs_pool_.size());
    page.swap(bufs_pool_[buf_id]);

    uint16_t buf_size = options_->data_page_size;
    int mask = io_uring_buf_ring_mask(options_->buf_ring_size);
    io_uring_buf_ring_add(
        buf_ring_, bufs_pool_[buf_id].get(), buf_size, buf_id, mask, 0);
    io_uring_buf_ring_advance(buf_ring_, 1);
    return page;
}

void IouringMgr::ReadPage(ReadReq *req)
{
    auto [fd, registered] = req->fd_ref_.FdPair();
    io_uring_sqe *sqe = GetSQE(UserDataType::ReadReq, req);
    if (registered)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->buf_group = buf_group_;
    sqe->flags |= IOSQE_BUFFER_SELECT;
    io_uring_prep_read(sqe, fd, NULL, 0, req->offset_);
}

int IouringMgr::Write(FdIdx fd, const char *src, size_t n, uint64_t offset)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_write(sqe, fd.first, src, n, offset);
    return thd_task->WaitSyncIo();
}

int IouringMgr::Fdatasync(LruFD::Ref fd)
{
    int res = Fdatasync(fd.FdPair());
    if (res >= 0)
    {
        fd.Get()->dirty_ = false;
    }
    return res;
}

int IouringMgr::Fdatasync(FdIdx fd)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_fsync(sqe, fd.first, IORING_FSYNC_DATASYNC);
    return thd_task->WaitSyncIo();
}

int IouringMgr::Rename(FdIdx dir_fd, const char *old_path, const char *new_path)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (dir_fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_renameat(
        sqe, dir_fd.first, old_path, dir_fd.first, new_path, 0);
    return thd_task->WaitSyncIo();
}

int IouringMgr::CloseFile(int fd)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    io_uring_prep_close(sqe, fd);
    int res = thd_task->WaitSyncIo();
    if (res < 0)
    {
        LOG(ERROR) << "close file/directory " << fd << " failed: " << res;
    }
    return res;
}

KvError IouringMgr::CloseFile(LruFD::Ref fd_ref)
{
    // This LruFD will be removed by ~LruFD::Ref if succeed to close, otherwise
    // be enqueued back to LRU.
    LruFD *lru_fd = fd_ref.Get();
    assert(lru_fd->ref_count_ == 1);
    FdIdx fd_idx = lru_fd->FdPair();
    int fd = lru_fd->fd_;

    // Make sure no tasks can use this fd during closing.
    lru_fd->fd_ = LruFD::FdLocked;

    if (lru_fd->dirty_)
    {
        if (int res = Fdatasync(fd_ref); res < 0)
        {
            lru_fd->fd_ = fd;
            return ToKvError(res);
        }
    }

    if (fd_idx.second)
    {
        UnregisterFile(fd_idx.first);
        lru_fd->reg_idx_ = -1;
    }

    if (int res = CloseFile(fd); res < 0)
    {
        lru_fd->fd_ = fd;
        return ToKvError(res);
    }
    lru_fd->fd_ = LruFD::FdEmpty;
    return KvError::NoError;
}

bool IouringMgr::EvictFD()
{
    while (lru_fd_count_ >= options_->fd_limit)
    {
        LruFD *lru_fd = lru_fd_tail_.prev_;
        if (lru_fd == &lru_fd_head_)
        {
            LOG(ERROR) << "too many opened files and can't evict";
            return false;
        }
        assert(lru_fd->ref_count_ == 0);
        CloseFile(LruFD::Ref(lru_fd, this));
    }
    return true;
}

uint32_t IouringMgr::AllocRegisterIndex()
{
    uint32_t idx = UINT32_MAX;
    if (free_reg_slots_.empty())
    {
        if (alloc_reg_slot_ < options_->fd_limit)
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
        LOG(ERROR) << "too many registered file ";
        return -1;
    }
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    io_uring_prep_files_update(sqe, &fd, 1, idx);
    int res = thd_task->WaitSyncIo();
    if (res < 0)
    {
        LOG(ERROR) << "failed to register file " << fd << " at " << idx;
        FreeRegisterIndex(idx);
        return -1;
    }
    return idx;
}

int IouringMgr::UnregisterFile(int idx)
{
    int fd = -1;
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    io_uring_prep_files_update(sqe, &fd, 1, idx);
    int res = thd_task->WaitSyncIo();
    if (res < 0)
    {
        LOG(ERROR) << "can't unregister file at " << idx << ": " << res;
    }

    FreeRegisterIndex(idx);
    return res;
}

int IouringMgr::Fallocate(FdIdx fd, uint64_t size)
{
    io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
    if (fd.second)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_prep_fallocate(sqe, fd.first, 0, 0, size);
    int res = thd_task->WaitSyncIo();
    if (res < 0)
    {
        LOG(ERROR) << "fallocate failed " << res;
    }
    return res;
}

KvError IouringMgr::AppendManifest(const TableIdent &tbl_id,
                                   std::string_view log,
                                   uint64_t manifest_size)
{
    TEST_KILL_POINT("AppendManifest:GetOrCreateFD")
    assert(manifest_size > 0);
    auto [fd_ref, err] = OpenFD(tbl_id, LruFD::kManifest);
    CHECK_KV_ERR(err);

    TEST_KILL_POINT("AppendManifest:Write")
    int res = Write(fd_ref.FdPair(), log.data(), log.size(), manifest_size);
    if (res < 0)
    {
        LOG(ERROR) << "append manifest failed " << tbl_id;
        return ToKvError(res);
    }

    TEST_KILL_POINT("AppendManifest:Fdatasync")
    res = Fdatasync(fd_ref.FdPair());
    if (res < 0)
    {
        LOG(ERROR) << "fsync manifest failed " << tbl_id;
        return ToKvError(res);
    }
    return KvError::NoError;
}

KvError IouringMgr::AtomicWriteFile(const TableIdent &tbl_id,
                                    const char *name,
                                    std::string_view content,
                                    LruFD::Ref result)
{
    // Generate temporary file.
    auto [tmp_ref, err] = OpenOrCreateFD(tbl_id, LruFD::kTmpFile);
    CHECK_KV_ERR(err);
    int res = Write(tmp_ref.FdPair(), content.data(), content.size(), 0);
    if (res < 0)
    {
        LOG(ERROR) << "write temporary file failed " << res;
        return ToKvError(res);
    }
    TEST_KILL_POINT("AtomicWriteFile:before_fsync")
    res = Fdatasync(tmp_ref.FdPair());
    if (res < 0)
    {
        LOG(ERROR) << "fsync temporary file failed " << res;
        return ToKvError(res);
    }
    TEST_KILL_POINT("AtomicWriteFile:after_fsync")

    // Switch file on disk.
    auto [dfd_ref, dir_err] = OpenFD(tbl_id, LruFD::kDirectory);
    CHECK_KV_ERR(dir_err);
    TEST_KILL_POINT("AtomicWriteFile:before_name")
    res = Rename(dfd_ref.FdPair(), FileNameTmpfile, name);
    if (res < 0)
    {
        LOG(ERROR) << "rename temporary file failed " << res;
        return ToKvError(res);
    }
    TEST_KILL_POINT("AtomicWriteFile:fsync_dir")
    res = Fdatasync(std::move(dfd_ref));
    if (res < 0)
    {
        LOG(ERROR) << "fsync directory failed " << res;
        return ToKvError(res);
    }

    if (result != nullptr)
    {
        // Switch file in memory
        result.Get()->fd_ = tmp_ref.Get()->fd_;
        result.Get()->reg_idx_ = tmp_ref.Get()->reg_idx_;
        assert(tmp_ref.Get()->ref_count_ == 1);
        tmp_ref.Get()->fd_ = LruFD::FdEmpty;
        tmp_ref.Get()->reg_idx_ = -1;
    }
    else
    {
        err = CloseFile(std::move(tmp_ref));
        CHECK(err == KvError::NoError);
    }
    return KvError::NoError;
}

KvError IouringMgr::SwitchManifest(const TableIdent &tbl_id,
                                   std::string_view snapshot)
{
    LOG(INFO) << "switch manifest file for " << tbl_id;
    // Unregister and close the old manifest if it is opened
    LruFD::Ref fd_ref = GetFDSlot(tbl_id, LruFD::kManifest);
    if (fd_ref == nullptr)
    {
        return KvError::OpenFileLimit;
    }
    TEST_KILL_POINT("SwitchManifest:0")
    auto [old_fd, registered] = fd_ref.FdPair();
    if (old_fd >= 0)
    {
        if (registered)
        {
            UnregisterFile(old_fd);
        }
        CloseFile(fd_ref.Get()->fd_);
        fd_ref.Get()->fd_ = LruFD::FdEmpty;
    }

    TEST_KILL_POINT("SwitchManifest:1")
    return AtomicWriteFile(
        tbl_id, FileNameManifest, snapshot, std::move(fd_ref));
}

KvError IouringMgr::CreateArchive(const TableIdent &tbl_id,
                                  std::string_view snapshot,
                                  uint64_t ts)
{
    const std::string name = ArchiveName(ts);
    return AtomicWriteFile(tbl_id, name.c_str(), snapshot, nullptr);
}

io_uring_sqe *IouringMgr::GetSQE(UserDataType type, const void *user_ptr)
{
    io_uring_sqe *sqe;
    while ((sqe = io_uring_get_sqe(&ring_)) == NULL)
    {
        waiting_sqe_.Sleep(thd_task);
    }

    if (user_ptr != nullptr)
    {
        EncodeUserData(sqe, user_ptr, type);
    }
    thd_task->inflight_io_++;
    not_submitted_sqe_++;
    return sqe;
}

void IouringMgr::FreeWriteReq(WriteReq *req)
{
    req->fd_ref_ = nullptr;

    WriteReq *first = free_write_reqs_.next_;
    free_write_reqs_.next_ = req;
    req->next_ = first;

    waiting_write_.WakeOne();
}

IouringMgr::WriteReq *IouringMgr::AllocWriteReq(LruFD::Ref fd, VarPage page)
{
    auto try_get_req = [this]() -> WriteReq *
    {
        WriteReq *first = free_write_reqs_.next_;
        if (first != nullptr)
        {
            free_write_reqs_.next_ = first->next_;
            first->next_ = nullptr;
        }
        else if (write_reqs_pool_.size() < options_->max_inflight_write)
        {
            write_reqs_pool_.emplace_back(std::make_unique<WriteReq>());
            first = write_reqs_pool_.back().get();
        }
        return first;
    };

    WriteReq *req = try_get_req();
    while (req == nullptr)
    {
        waiting_write_.Sleep(thd_task);
        req = try_get_req();
    }
    req->fd_ref_ = std::move(fd);
    req->SetPage(std::move(page));
    req->task_ = static_cast<WriteTask *>(thd_task);
    return req;
}

KvError IouringMgr::Manifest::Read(char *dst, size_t n)
{
    // TODO(zhanghao): Use provided buffer to read 4KB at a time.
    while (n > 0)
    {
        int res = io_mgr_->Read(fd_.FdPair(), dst, n, offset_);
        assert(res <= n);
        if (res < 0)
        {
            if (res == -EAGAIN)
            {
                continue;
            }
            return ToKvError(res);
        }
        if (res == 0)
        {
            return KvError::EndOfFile;
        }
        dst += res;
        offset_ += res;
        n -= res;
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
            io_mgr_->lru_fd_count_--;
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

IouringMgr::ReadReq::ReadReq(KvTask *task, LruFD::Ref fd, uint32_t offset)
    : task_(task),
      fd_ref_(std::move(fd)),
      offset_(offset),
      page_(shard->PagePool()->Allocate())
{
}

IouringMgr::ReadReq::~ReadReq()
{
    if (page_ != nullptr)
    {
        shard->PagePool()->Free(std::move(page_));
    }
}

char *IouringMgr::WriteReq::PagePtr() const
{
    return VarPagePtr(page_);
}

void IouringMgr::WriteReq::SetPage(VarPage page)
{
    switch (page.index())
    {
    case 0:
        page_.emplace<MemIndexPage *>(std::get<MemIndexPage *>(page));
        break;
    case 1:
        page_.emplace<DataPage>(std::move(std::get<DataPage>(page)));
        break;
    case 2:
        page_.emplace<OverflowPage>(std::move(std::get<OverflowPage>(page)));
        break;
    case 3:
        page_.emplace<Page>(std::move(std::get<Page>(page)));
        break;
    }
}

MemStoreMgr::MemStoreMgr(const KvOptions *opts) : AsyncIoManager(opts)
{
}

KvError MemStoreMgr::Init(int dir_fd)
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
    memcpy(page.get(), part.pages[fp_id].get(), options_->data_page_size);
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
        auto [page, err] =
            ReadPage(tbl_id, fp_id, shard->PagePool()->Allocate());
        pages.push_back(std::move(page));
        if (err != KvError::NoError)
        {
            while (!pages.empty())
            {
                shard->PagePool()->Free(std::move(pages.back()));
                pages.pop_back();
            }
            return err;
        }
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

    static_cast<WriteTask *>(thd_task)->WritePageCallback(std::move(page),
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

}  // namespace kvstore