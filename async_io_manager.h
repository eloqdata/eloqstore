#pragma once

#include <glog/logging.h>
#include <liburing.h>
#include <sys/types.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "circular_queue.h"
#include "error.h"
#include "kv_options.h"
#include "table_ident.h"
#include "task.h"

namespace kvstore
{
class WriteReq;
class WriteTask;
class MemIndexPage;

class ManifestFile
{
public:
    virtual ~ManifestFile() = default;
    virtual int Read(char *dst, size_t n) = 0;
};

using ManifestFilePtr = std::unique_ptr<ManifestFile>;

using VarPage = std::variant<MemIndexPage *, DataPage>;

class AsyncIoManager
{
public:
    AsyncIoManager(const KvOptions *opts) : options_(opts) {};
    virtual ~AsyncIoManager() = default;
    static std::unique_ptr<AsyncIoManager> New(const KvOptions *opts);
    /** These methods are used by worker thread. */
    virtual KvError Init(int dir_fd) = 0;
    virtual void Submit() = 0;
    virtual void PollComplete() = 0;

    /** These methods are used by read/write task. */
    virtual std::pair<std::unique_ptr<char[]>, KvError> ReadPage(
        const TableIdent &tbl_id,
        uint32_t fp_id,
        std::unique_ptr<char[]> page) = 0;
    virtual KvError WritePage(const TableIdent &tbl_id,
                              VarPage page,
                              uint32_t file_page_id) = 0;
    virtual KvError FlushData(const TableIdent &tbl_id) = 0;
    virtual KvError AppendManifest(const TableIdent &tbl_id,
                                   std::string_view log,
                                   uint64_t manifest_size) = 0;
    virtual KvError SwitchManifest(const TableIdent &tbl_id,
                                   std::string_view snapshot) = 0;
    virtual ManifestFilePtr GetManifest(const TableIdent &tbl_id) = 0;
    virtual void CleanTable(const TableIdent &tbl_id) = 0;

    const KvOptions *options_;
};

class IouringMgr : public AsyncIoManager
{
public:
    IouringMgr(const KvOptions *opts);
    ~IouringMgr() override;
    KvError Init(int dir_fd) override;
    void Submit() override;
    void PollComplete() override;

    std::pair<std::unique_ptr<char[]>, KvError> ReadPage(
        const TableIdent &tbl_id,
        uint32_t fp_id,
        std::unique_ptr<char[]> page) override;
    KvError WritePage(const TableIdent &tbl_id,
                      VarPage page,
                      uint32_t file_page_id) override;
    KvError FlushData(const TableIdent &tbl_id) override;
    KvError AppendManifest(const TableIdent &tbl_id,
                           std::string_view log,
                           uint64_t manifest_size) override;
    KvError SwitchManifest(const TableIdent &tbl_id,
                           std::string_view snapshot) override;
    ManifestFilePtr GetManifest(const TableIdent &tbl_id) override;
    void CleanTable(const TableIdent &tbl_id) override;

    static std::string DataFileName(uint32_t file_id);

    static constexpr char mani_file[] = "manifest";
    static constexpr char mani_tmpfile[] = "manifest.tmp";
    static constexpr uint64_t oflags_dir = O_DIRECTORY | O_RDONLY;

private:
    enum class UserDataType : uint8_t
    {
        KvTask,
        AsynWriteReq,
        AsynFsyncReq
    };

    class PartitionFiles;
    using FdIdx = std::pair<int, bool>;
    class LruFD
    {
    public:
        class Ref
        {
        public:
            Ref(LruFD *fd_ptr = nullptr, IouringMgr *io_mgr = nullptr);
            Ref(Ref &&other) noexcept;
            Ref &operator=(Ref &&other) noexcept;
            ~Ref();
            bool operator==(const Ref &other) const;
            // Delete copy operations to ensure RAII semantics
            Ref(const Ref &) = delete;
            Ref &operator=(const Ref &) = delete;
            FdIdx FdPair() const;
            LruFD *Get() const;

        private:
            void Clear();
            LruFD *fd_ = nullptr;
            IouringMgr *io_mgr_ = nullptr;
        };

        LruFD(PartitionFiles *tbl, uint32_t file_id);
        FdIdx FdPair() const;
        void Deque();
        LruFD *DequeNext();
        LruFD *DequePrev();
        void EnqueNext(LruFD *new_fd);

        static constexpr uint32_t kDirectiry = UINT32_MAX;
        static constexpr uint32_t kManifest = UINT32_MAX - 1;
        static constexpr uint32_t kTmpManifest = UINT32_MAX - 2;

        static constexpr int FdEmpty = -1;
        static constexpr int FdLocked = -2;

        int fd_{FdEmpty};
        int reg_idx_{-1};
        bool dirty_{false};

        PartitionFiles *const tbl_;
        const uint32_t file_id_;
        uint32_t ref_count_{0};
        std::vector<KvTask *> loading_;
        LruFD *prev_{nullptr};
        LruFD *next_{nullptr};
    };

    struct FsyncReq
    {
        FsyncReq(KvTask *task, LruFD *fd_ptr, IouringMgr *io_mgr)
            : task_(task), fd_ref_(fd_ptr, io_mgr) {};
        KvTask *task_;
        LruFD::Ref fd_ref_;
    };

    struct WriteReq
    {
        char *PagePtr() const;
        VarPage page_;
        LruFD::Ref fd_ref_;
        WriteTask *task_{nullptr};
        WriteReq *next_{nullptr};
    };

    class PartitionFiles
    {
    public:
        const TableIdent *tbl_id_ = nullptr;
        std::unordered_map<uint32_t, LruFD> fds_;
    };

    class Manifest : public ManifestFile
    {
    public:
        Manifest(IouringMgr *io_mgr, LruFD::Ref &&fd)
            : io_mgr_(io_mgr), fd_(std::move(fd)) {};
        int Read(char *dst, size_t n) override;

    private:
        IouringMgr *io_mgr_;
        LruFD::Ref fd_;
        uint64_t offset_{0};
        // TODO(zhanghao): use selected ring buffer API
        // char *buff_; // options_->data_page_size
    };

    static KvError ToKvError(int err_no);
    static std::pair<void *, UserDataType> DecodeUserData(uint64_t user_data);
    static void EncodeUserData(io_uring_sqe *sqe, void *ptr, UserDataType type);
    /**
     * @brief Convert file page id to <file_id, file_offset>
     */
    std::pair<uint32_t, uint32_t> ConvFilePageId(uint32_t file_page_id) const;
    uint32_t AllocRegisterIndex();
    void FreeRegisterIndex(uint32_t idx);

    io_uring_sqe *GetSQE(UserDataType type = UserDataType::KvTask,
                         void *user_ptr = nullptr);
    // Low-level io operation
    int CreateDir(FdIdx dir_fd, const char *path);
    int OpenAt(FdIdx dir_fd,
               const char *path,
               uint64_t flags,
               uint64_t mode = 0);
    int Read(FdIdx fd, char *dst, size_t n, uint64_t offset);
    std::pair<std::unique_ptr<char[]>, int> Read(FdIdx fd,
                                                 std::unique_ptr<char[]> ptr,
                                                 uint32_t offset);
    int Write(FdIdx fd, const char *src, size_t n, uint64_t offset);
    int Fdatasync(FdIdx fd);
    int Rename(FdIdx dir_fd, const char *old_path, const char *new_path);
    int CloseFile(int fd);
    int RegisterFile(int fd);
    int UnregisterFile(int idx);

    LruFD::Ref GetCachedFD(const TableIdent &tbl_id, uint32_t file_id);
    LruFD::Ref GetFD(const TableIdent &tbl_id, uint32_t file_id);
    LruFD::Ref GetOrCreateFD(const TableIdent &tbl_id,
                             uint32_t file_id,
                             bool create = true);
    bool EvictFDIfFull();

    WriteReq *AllocWriteReq();
    void FreeWriteReq(WriteReq *req);
    std::vector<std::unique_ptr<WriteReq>> asyn_write_reqs_;
    WriteReq free_asyn_write_;

    FdIdx dir_fd_idx_{-1, false};
    std::unordered_map<TableIdent, PartitionFiles> tables_;
    LruFD lru_fd_head_{nullptr, UINT32_MAX};
    LruFD lru_fd_tail_{nullptr, UINT32_MAX};
    uint32_t lru_fd_count_{0};

    uint32_t alloc_reg_slot_{0};
    std::vector<uint32_t> free_reg_slots_;

    io_uring_buf_ring *buf_ring_{nullptr};
    std::vector<std::unique_ptr<char[]>> bufs_pool_;
    const int buf_group_{0};

    io_uring ring_;
    CircularQueue<KvTask *> waiting_sqe_;
    uint32_t not_submitted_sqe_{0};
};

class MemStoreMgr : public AsyncIoManager
{
public:
    MemStoreMgr(const KvOptions *opts);
    KvError Init(int dir_fd) override;
    void Submit() override {};
    void PollComplete() override {};

    std::pair<std::unique_ptr<char[]>, KvError> ReadPage(
        const TableIdent &tbl_id,
        uint32_t file_page_id,
        std::unique_ptr<char[]> page) override;
    KvError WritePage(const TableIdent &tbl_id,
                      VarPage page,
                      uint32_t file_page_id) override;
    KvError FlushData(const TableIdent &tbl_id) override
    {
        return KvError::NoError;
    }
    void CleanTable(const TableIdent &tbl_id) override;

    KvError AppendManifest(const TableIdent &tbl_id,
                           std::string_view log,
                           uint64_t manifest_size) override;
    KvError SwitchManifest(const TableIdent &tbl_id,
                           std::string_view snapshot) override;
    ManifestFilePtr GetManifest(const TableIdent &tbl_id) override;

    class Manifest : public ManifestFile
    {
    public:
        Manifest(std::string_view content) : content_(content) {};
        int Read(char *dst, size_t n) override;
        void Skip(size_t n);

    private:
        std::string_view content_;
    };

private:
    struct Partition
    {
        std::vector<std::unique_ptr<char[]>> pages;
        std::string wal;
    };
    std::unordered_map<TableIdent, Partition> store_;
};

}  // namespace kvstore
