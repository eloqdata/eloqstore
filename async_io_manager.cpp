#include "async_io_manager.h"

#include <dirent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <linux/openat2.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "read_task.h"
#include "table_ident.h"
#include "task.h"
#include "write_task.h"

namespace kvstore
{
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
        char *buf = (char *) std::aligned_alloc(page_align, buf_size);
        if (buf == nullptr)
        {
            LOG(ERROR) << "failed to allocate memory for buffer ring";
            io_uring_unregister_files(&ring_);
            io_uring_queue_exit(&ring_);
            return KvError::OutOfMem;
        }
        bufs_pool_.emplace_back(std::unique_ptr<char[]>(buf));
        io_uring_buf_ring_add(buf_ring_, buf, buf_size, i, mask, i);
    }
    io_uring_buf_ring_advance(buf_ring_, num_bufs);

    dir_fd_idx_ = {dir_fd, false};
    return KvError::NoError;
}

std::pair<std::unique_ptr<char[]>, KvError> IouringMgr::ReadPage(
    const TableIdent &tbl_id, uint32_t fp_id, std::unique_ptr<char[]> page)
{
    auto [file_id, offset] = ConvFilePageId(fp_id);
    LruFD::Ref fd_ref = GetFD(tbl_id, file_id);
    if (fd_ref == nullptr)
    {
        return {std::move(page), KvError::NotFound};
    }

    int res;
    do
    {
        auto ret_pair = Read(fd_ref.FdPair(), std::move(page), offset);
        page = std::move(ret_pair.first);
        res = ret_pair.second;
    } while (res == -ENOBUFS);

    if (res < options_->data_page_size)
    {
        KvError err = res < 0 ? ToKvError(res) : KvError::TryAgain;
        return {std::move(page), err};
    }

    if (!ValidatePageCrc32(page.get(), options_->data_page_size))
    {
        LOG(ERROR) << "corrupted " << tbl_id << " page " << fp_id;
        return {std::move(page), KvError::Corrupted};
    }
    return {std::move(page), KvError::NoError};
}

ManifestFilePtr IouringMgr::GetManifest(const TableIdent &tbl_id)
{
    LruFD::Ref fd = GetFD(tbl_id, LruFD::kManifest);
    if (fd == nullptr)
    {
        return nullptr;
    }
    return std::make_unique<Manifest>(this, std::move(fd));
}

KvError IouringMgr::WritePage(const TableIdent &tbl_id,
                              VarPage page,
                              uint32_t file_page_id)
{
    auto [file_id, offset] = ConvFilePageId(file_page_id);
    LruFD::Ref fd_ref = GetOrCreateFD(tbl_id, file_id);
    if (fd_ref == nullptr)
    {
        return KvError::OpenFileLimit;
    }

    auto [fd, registered] = fd_ref.FdPair();
    io_uring_sqe *sqe = GetSQE();
    if (registered)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    // We allocate a AsynWriteReq after we get a SQE so that the size of
    // WriteReq object pool is limited too.
    WriteReq *req = AllocWriteReq();
    req->page_.swap(page);
    EncodeUserData(sqe, req, UserDataType::AsynWriteReq);

    char *ptr = req->PagePtr();
    SetPageCrc32(ptr, Options()->data_page_size);
    io_uring_prep_write(sqe, fd, ptr, options_->data_page_size, offset);
    req->fd_ref_ = std::move(fd_ref);
    return KvError::NoError;
}

KvError IouringMgr::FlushData(const TableIdent &tbl_id)
{
    int err = thd_task->WaitAsynIo();
    if (err < 0)
    {
        LOG(ERROR) << "write data files failed " << tbl_id << " : " << err;
        return ToKvError(err);
    }

    auto it_tbl = tables_.find(tbl_id);
    if (it_tbl == tables_.end())
    {
        LOG(WARNING) << "sync table partition not found " << tbl_id;
        return KvError::NoError;
    }

    std::vector<FsyncReq> fsync_reqs;
    for (auto &[id, fd] : it_tbl->second.fds_)
    {
        if (fd.dirty_)
        {
            fsync_reqs.emplace_back(thd_task, &fd, this);
        }
    }
    for (FsyncReq &req : fsync_reqs)
    {
        auto [fd, registered] = req.fd_ref_.FdPair();
        io_uring_sqe *sqe = GetSQE(UserDataType::AsynFsyncReq, &req);
        if (registered)
        {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
    }
    err = thd_task->WaitAsynIo();
    if (err < 0)
    {
        LOG(ERROR) << "fsync data files failed " << tbl_id << ':' << err;
        return ToKvError(err);
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

std::string IouringMgr::DataFileName(uint32_t file_id)
{
    std::string name = "data_";
    std::stringstream ss;
    ss << std::setw(3) << std::setfill('0') << file_id;
    name.append(ss.str());
    return name;
}

KvError IouringMgr::ToKvError(int err_no)
{
    assert(err_no < 0);
    switch (err_no)
    {
    case -ENOENT:
        return KvError::NotFound;
    case -EAGAIN:
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
                                void *ptr,
                                IouringMgr::UserDataType type)
{
    void *user_data = (void *) ((uint64_t(ptr) << 8) | uint64_t(type));
    io_uring_sqe_set_data(sqe, user_data);
}

IouringMgr::LruFD::Ref IouringMgr::GetCachedFD(const TableIdent &tbl_id,
                                               uint32_t file_id)
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
            if (!EvictFDIfFull())
            {
                return nullptr;
            }
            // EvictFDIfFull will give up the cpu and resume, so tbl has a
            // chance to become invalid.
            tbl = get_tbl_partition(tbl_id);
        }

        lru_fd_count_++;
        auto [it, _] = tbl->fds_.try_emplace(file_id, tbl, file_id);
        it_fd = it;
    }
    return {&it_fd->second, this};
}

IouringMgr::LruFD::Ref IouringMgr::GetFD(const TableIdent &tbl_id,
                                         uint32_t file_id)
{
    return GetOrCreateFD(tbl_id, file_id, false);
}

IouringMgr::LruFD::Ref IouringMgr::GetOrCreateFD(const TableIdent &tbl_id,
                                                 uint32_t file_id,
                                                 bool create)
{
    LruFD::Ref lru_fd = GetCachedFD(tbl_id, file_id);
    if (lru_fd == nullptr)
    {
        return nullptr;
    }

    while (lru_fd.Get()->fd_ < 0)
    {
        switch (lru_fd.Get()->fd_)
        {
        case LruFD::FdEmpty:
        {
            lru_fd.Get()->fd_ = LruFD::FdLocked;

            int fd;
            if (file_id == LruFD::kDirectiry)
            {
                std::string dirname = tbl_id.ToString();
                fd = OpenAt(dir_fd_idx_, dirname.c_str(), oflags_dir);
                if (fd == -ENOENT && create)
                {
                    fd = CreateDir(dir_fd_idx_, dirname.c_str());
                }
            }
            else if (file_id == LruFD::kManifest ||
                     file_id == LruFD::kTmpManifest)
            {
                std::string path = tbl_id.ToString();
                path.push_back('/');
                const char *filename =
                    file_id == LruFD::kManifest ? mani_file : mani_tmpfile;
                path.append(filename);
                fd = OpenAt(dir_fd_idx_, path.c_str(), O_RDWR);
                if (fd == -ENOENT && create)
                {
                    LruFD::Ref dfd_ref =
                        GetOrCreateFD(tbl_id, LruFD::kDirectiry);
                    if (dfd_ref != nullptr)
                    {
                        fd = OpenAt(dfd_ref.FdPair(),
                                    filename,
                                    O_CREAT | O_TRUNC | O_RDWR,
                                    0644);
                        if (fd >= 0 && Fdatasync(dfd_ref.FdPair()) < 0)
                        {
                            LOG(ERROR) << "create " << path
                                       << " can't fsync directory";
                        }
                    }
                }
            }
            else
            {
                std::string filename = DataFileName(file_id);
                std::string path = tbl_id.ToString();
                path.push_back('/');
                path.append(filename);

                fd = OpenAt(dir_fd_idx_, path.c_str(), O_RDWR | O_DIRECT);
                if (fd == -ENOENT && create)
                {
                    LruFD::Ref dfd_ref =
                        GetOrCreateFD(tbl_id, LruFD::kDirectiry);
                    if (dfd_ref != nullptr)
                    {
                        fd = OpenAt(dfd_ref.FdPair(),
                                    filename.c_str(),
                                    O_CREAT | O_RDWR | O_DIRECT,
                                    0644);
                        if (fd >= 0)
                        {
                            // Multiple data files maybe created in one
                            // WriteTask. Table partition directory FD will be
                            // marked as dirty every time, but only fsync it
                            // once when SyncFiles.
                            dfd_ref.Get()->dirty_ = true;
                        }
                    }
                }
            }

            if (fd < 0)
            {
                LOG(ERROR) << "open file/directory failed " << tbl_id << '@'
                           << file_id;
                lru_fd.Get()->fd_ = LruFD::FdEmpty;
                // notify all waiting tasks failed
                for (KvTask *task : lru_fd.Get()->loading_)
                {
                    task->io_res_ = fd;
                    task->Resume();
                }
                lru_fd.Get()->loading_.clear();
                return nullptr;
            }

            if (file_id != LruFD::kDirectiry)
            {
                lru_fd.Get()->reg_idx_ = RegisterFile(fd);
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
            lru_fd.Get()->loading_.push_back(thd_task);
            thd_task->status_ = TaskStatus::Blocked;
            thd_task->Yield();
            if (thd_task->io_res_ < 0)
            {
                return nullptr;
            }
            break;
        default:
            assert(false);
        }
    }

    return std::move(lru_fd);
}

std::pair<uint32_t, uint32_t> IouringMgr::ConvFilePageId(
    uint32_t file_page_id) const
{
    uint32_t file_id = file_page_id >> options_->num_file_pages_shift;
    uint32_t offset =
        (file_page_id & ((1 << options_->num_file_pages_shift) - 1)) *
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

    int ret = io_uring_submit(&ring_);
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
        DLOG_IF(INFO, cqe->res < 0) << "iouring operation failed " << cqe->res;
        cnt++;

        auto [ptr, typ] = DecodeUserData(cqe->user_data);
        bool is_sync_io;
        KvTask *task;
        switch (typ)
        {
        case UserDataType::KvTask:
            task = static_cast<KvTask *>(ptr);
            is_sync_io = true;
            break;
        case UserDataType::AsynWriteReq:
        {
            WriteReq *req = static_cast<WriteReq *>(ptr);
            LruFD::Ref ref = std::move(req->fd_ref_);
            if (cqe->res >= 0)
            {
                ref.Get()->dirty_ = true;
            }
            req->task_->WritePageCallback(std::move(req->page_));
            FreeWriteReq(req);

            task = req->task_;
            is_sync_io = false;
            break;
        }
        case UserDataType::AsynFsyncReq:
        {
            FsyncReq *req = static_cast<FsyncReq *>(ptr);
            LruFD::Ref ref = std::move(req->fd_ref_);
            if (cqe->res >= 0)
            {
                ref.Get()->dirty_ = false;
            }

            task = req->task_;
            is_sync_io = false;
            break;
        }
        default:
            assert(false);
        }
        if (is_sync_io)
        {
            task->io_res_ = cqe->res;
            task->io_flags_ = cqe->flags;
        }
        else if (cqe->res < 0)
        {
            // asyn_io_err_ will record the error code of the last failed
            // asynchronous IO request.
            task->asyn_io_err_ = cqe->res;
        }
        task->FinishIo(is_sync_io);
    }

    io_uring_cq_advance(&ring_, cnt);

    for (size_t i = 0; i < cnt && waiting_sqe_.Size() > 0; i++)
    {
        KvTask *task = waiting_sqe_.Peek();
        waiting_sqe_.Dequeue();
        task->Resume();
    }
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

std::pair<std::unique_ptr<char[]>, int> IouringMgr::Read(
    FdIdx fd, std::unique_ptr<char[]> page, uint32_t offset)
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
        assert(ret > 0);
        uint16_t buf_id = thd_task->io_flags_ >> IORING_CQE_BUFFER_SHIFT;
        assert(buf_id < bufs_pool_.size());
        page.swap(bufs_pool_[buf_id]);

        uint16_t buf_size = options_->data_page_size;
        int mask = io_uring_buf_ring_mask(options_->buf_ring_size);
        io_uring_buf_ring_add(
            buf_ring_, bufs_pool_[buf_id].get(), buf_size, buf_id, mask, 0);
        io_uring_buf_ring_advance(buf_ring_, 1);
    }
    else
    {
        assert(ret <= 0);
    }
    return {std::move(page), ret};
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

bool IouringMgr::EvictFDIfFull()
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

        // This fd will be enqueue again by ~Guard() when an error occurs
        LruFD::Ref ref(lru_fd, this);

        FdIdx fd_idx = lru_fd->FdPair();
        int fd = lru_fd->fd_;
        lru_fd->fd_ = LruFD::FdLocked;

        if (lru_fd->dirty_)
        {
            if (Fdatasync(fd_idx) < 0)
            {
                LOG(ERROR) << "failed to fsync before evict fd";
                lru_fd->fd_ = fd;
                continue;
            }
            lru_fd->dirty_ = false;
        }

        if (fd_idx.second)
        {
            UnregisterFile(fd_idx.first);
            lru_fd->reg_idx_ = -1;
        }

        if (CloseFile(fd) < 0)
        {
            LOG(ERROR) << "failed to close before evict fd";
            lru_fd->fd_ = fd;
            continue;
        }
        lru_fd->fd_ = LruFD::FdEmpty;
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

// uint64_t IouringMgr::GetFileSize(int dfd, const char *path)
// {
//     io_uring_sqe *sqe = GetSQE(UserDataType::KvTask, thd_task);
//     struct statx stx;
//     io_uring_prep_statx(sqe, dfd, path, 0, STATX_SIZE, &stx);
//     int res = thd_task->WaitIo();
//     if (res < 0)
//     {
//         LOG(ERROR) << "statx " << dfd << '/' << path << " failed " << res;
//         return UINT64_MAX;
//     }
//     return stx.stx_size;
// }

KvError IouringMgr::AppendManifest(const TableIdent &tbl_id,
                                   std::string_view log,
                                   uint64_t manifest_size)
{
    assert(manifest_size > 0);
    LruFD::Ref fd_ref = GetOrCreateFD(tbl_id, LruFD::kManifest);
    if (fd_ref == nullptr)
    {
        return KvError::OpenFileLimit;
    }

    int res = Write(fd_ref.FdPair(), log.data(), log.size(), manifest_size);
    if (res < 0)
    {
        LOG(ERROR) << "append manifest failed " << tbl_id;
        return ToKvError(res);
    }

    res = Fdatasync(fd_ref.FdPair());
    if (res < 0)
    {
        LOG(ERROR) << "fsync manifest failed " << tbl_id;
        return ToKvError(res);
    }
    return KvError::NoError;
}

KvError IouringMgr::SwitchManifest(const TableIdent &tbl_id,
                                   std::string_view snapshot)
{
    LOG(INFO) << "switch manifest file for " << tbl_id;
    // Unregister and close the old manifest if it is opened
    LruFD::Ref fd_ref = GetCachedFD(tbl_id, LruFD::kManifest);
    if (fd_ref == nullptr)
    {
        return KvError::OpenFileLimit;
    }
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

    // Generate temporary manifest
    LruFD::Ref tmp_ref = GetOrCreateFD(tbl_id, LruFD::kTmpManifest);
    int res = Write(tmp_ref.FdPair(), snapshot.data(), snapshot.size(), 0);
    if (res < 0)
    {
        LOG(ERROR) << "init temporary manifest write failed " << res;
        return ToKvError(res);
    }
    res = Fdatasync(tmp_ref.FdPair());
    if (res < 0)
    {
        LOG(ERROR) << "init temporary manifest fsync failed " << res;
        return ToKvError(res);
    }

    // Switch manifest on disk
    LruFD::Ref dfd_ref = GetOrCreateFD(tbl_id, LruFD::kDirectiry);
    if (dfd_ref == nullptr)
    {
        return KvError::IoFail;
    }
    res = Rename(dfd_ref.FdPair(), mani_tmpfile, mani_file);
    if (res < 0)
    {
        LOG(ERROR) << "switch manifest rename failed " << res;
        return ToKvError(res);
    }
    res = Fdatasync(dfd_ref.FdPair());
    if (res < 0)
    {
        LOG(ERROR) << "switch manifest fsync directory failed " << res;
        return ToKvError(res);
    }

    // Switch manifest in memory
    fd_ref.Get()->fd_ = tmp_ref.Get()->fd_;
    fd_ref.Get()->reg_idx_ = tmp_ref.Get()->reg_idx_;
    tmp_ref.Get()->fd_ = LruFD::FdEmpty;
    tmp_ref.Get()->reg_idx_ = -1;
    return KvError::NoError;
}

io_uring_sqe *IouringMgr::GetSQE(UserDataType type, void *user_ptr)
{
    io_uring_sqe *sqe;
    while ((sqe = io_uring_get_sqe(&ring_)) == NULL)
    {
        waiting_sqe_.Enqueue(thd_task);
        thd_task->status_ = TaskStatus::Blocked;
        thd_task->Yield();
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
    assert(req->fd_ref_.Get() == nullptr);
    WriteReq *first = free_asyn_write_.next_;
    free_asyn_write_.next_ = req;
    req->next_ = first;
}

IouringMgr::WriteReq *IouringMgr::AllocWriteReq()
{
    WriteReq *first = free_asyn_write_.next_;
    if (first != nullptr)
    {
        free_asyn_write_.next_ = first->next_;
        first->next_ = nullptr;
    }
    else
    {
        // IO uring submission queue entry is acquired first and the submission
        // queue size is fixed, so the maximal ongoing write requests is capped.
        asyn_write_reqs_.emplace_back(std::make_unique<WriteReq>());
        first = asyn_write_reqs_.back().get();
        assert(asyn_write_reqs_.size() <= options_->io_queue_size);
    }

    first->task_ = static_cast<WriteTask *>(thd_task);
    return first;
}

int IouringMgr::Manifest::Read(char *dst, size_t n)
{
    int res = io_mgr_->Read(fd_.FdPair(), dst, n, offset_);
    if (res < 0)
    {
        return res;
    }
    offset_ += res;
    return res;
}

IouringMgr::LruFD::LruFD(PartitionFiles *tbl, uint32_t file_id)
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

char *IouringMgr::WriteReq::PagePtr() const
{
    char *ptr = page_.index() == 0 ? std::get<0>(page_)->PagePtr()
                                   : std::get<1>(page_).PagePtr();
    assert(!((uint64_t) ptr & (page_align - 1)));
    return ptr;
}

MemStoreMgr::MemStoreMgr(const KvOptions *opts) : AsyncIoManager(opts)
{
}

KvError MemStoreMgr::Init(int dir_fd)
{
    return KvError::NoError;
}

std::pair<std::unique_ptr<char[]>, KvError> MemStoreMgr::ReadPage(
    const TableIdent &tbl_id, uint32_t fp_id, std::unique_ptr<char[]> page)
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

ManifestFilePtr MemStoreMgr::GetManifest(const TableIdent &tbl_ident)
{
    auto it = store_.find(tbl_ident);
    if (it == store_.end())
    {
        return nullptr;
    }
    return std::make_unique<Manifest>(it->second.wal);
}

KvError MemStoreMgr::WritePage(const TableIdent &tbl_id,
                               VarPage page,
                               uint32_t file_page_id)
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
    char *ptr = page.index() == 0 ? std::get<0>(page)->PagePtr()
                                  : std::get<1>(page).PagePtr();
    memcpy(dst, ptr, options_->data_page_size);

    static_cast<WriteTask *>(thd_task)->WritePageCallback(std::move(page));
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

int MemStoreMgr::Manifest::Read(char *dst, size_t n)
{
    size_t n0 = std::min(content_.length(), n);
    memcpy(dst, content_.data(), n0);
    content_ = content_.substr(n0);
    return n0;
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