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
#include <variant>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "object_store.h"
#include "prewarm_task.h"
#include "task.h"
#include "types.h"

namespace eloqstore
{
class WriteReq;
class WriteTask;
class MemIndexPage;
class PrewarmTask;

class ManifestFile
{
public:
    virtual ~ManifestFile() = default;
    virtual KvError Read(char *dst, size_t n) = 0;
};

using ManifestFilePtr = std::unique_ptr<ManifestFile>;

// TODO(zhanghao): consider using inheritance instead of variant
using VarPage = std::variant<MemIndexPage *, DataPage, OverflowPage, Page>;
char *VarPagePtr(const VarPage &page);
enum class VarPageType : uint8_t
{
    MemIndexPage = 0,
    DataPage,
    OverflowPage,
    Page
};

class EloqStore;

class AsyncIoManager
{
public:
    explicit AsyncIoManager(const KvOptions *opts) : options_(opts) {};
    virtual ~AsyncIoManager() = default;
    static std::unique_ptr<AsyncIoManager> Instance(const EloqStore *store,
                                                    uint32_t fd_limit);

    /** These methods are provided for worker thread. */
    virtual KvError Init(Shard *shard) = 0;
    virtual void Start() {};
    virtual bool IsIdle();
    virtual void Stop() {};
    virtual void Submit() = 0;
    virtual void PollComplete() = 0;
    virtual bool NeedPrewarm() const
    {
        return false;
    }
    virtual void RunPrewarm() {};

    /** These methods are provided for kv task. */
    virtual std::pair<Page, KvError> ReadPage(const TableIdent &tbl_id,
                                              FilePageId fp_id,
                                              Page page) = 0;
    virtual KvError ReadPages(const TableIdent &tbl_id,
                              std::span<FilePageId> page_ids,
                              std::vector<Page> &pages) = 0;

    virtual KvError WritePage(const TableIdent &tbl_id,
                              VarPage page,
                              FilePageId file_page_id) = 0;
    virtual KvError WritePages(const TableIdent &tbl_id,
                               std::span<VarPage> pages,
                               FilePageId first_fp_id) = 0;
    virtual KvError SyncData(const TableIdent &tbl_id) = 0;
    virtual KvError AbortWrite(const TableIdent &tbl_id) = 0;

    virtual KvError AppendManifest(const TableIdent &tbl_id,
                                   std::string_view log,
                                   uint64_t manifest_size) = 0;
    virtual KvError SwitchManifest(const TableIdent &tbl_id,
                                   std::string_view snapshot) = 0;
    virtual KvError CreateArchive(const TableIdent &tbl_id,
                                  std::string_view snapshot,
                                  uint64_t ts) = 0;
    virtual std::pair<ManifestFilePtr, KvError> GetManifest(
        const TableIdent &tbl_id) = 0;

    virtual void CleanManifest(const TableIdent &tbl_id) = 0;

    const KvOptions *options_;

    std::unordered_map<TableIdent, FileId> least_not_archived_file_ids_;
};

KvError ToKvError(int err_no);

class IouringMgr : public AsyncIoManager
{
public:
    IouringMgr(const KvOptions *opts, uint32_t fd_limit);
    ~IouringMgr() override;
    KvError Init(Shard *shard) override;
    void Submit() override;
    void PollComplete() override;

    std::pair<Page, KvError> ReadPage(const TableIdent &tbl_id,
                                      FilePageId fp_id,
                                      Page page) override;
    KvError ReadPages(const TableIdent &tbl_id,
                      std::span<FilePageId> page_ids,
                      std::vector<Page> &pages) override;

    KvError WritePage(const TableIdent &tbl_id,
                      VarPage page,
                      FilePageId file_page_id) override;
    KvError WritePages(const TableIdent &tbl_id,
                       std::span<VarPage> pages,
                       FilePageId first_fp_id) override;
    KvError SyncData(const TableIdent &tbl_id) override;
    KvError AbortWrite(const TableIdent &tbl_id) override;

    KvError AppendManifest(const TableIdent &tbl_id,
                           std::string_view log,
                           uint64_t manifest_size) override;
    KvError SwitchManifest(const TableIdent &tbl_id,
                           std::string_view snapshot) override;
    KvError CreateArchive(const TableIdent &tbl_id,
                          std::string_view snapshot,
                          uint64_t ts) override;
    std::pair<ManifestFilePtr, KvError> GetManifest(
        const TableIdent &tbl_id) override;

    KvError ReadArchiveFile(const std::string &file_path, std::string &content);
    KvError DeleteFiles(const std::vector<std::string> &file_paths);

    void CleanManifest(const TableIdent &tbl_id) override;

    static constexpr uint64_t oflags_dir = O_DIRECTORY | O_RDONLY;

protected:
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
            Ref(const Ref &);
            Ref &operator=(const Ref &) = delete;
            Ref &operator=(Ref &&other) noexcept;
            ~Ref();
            bool operator==(const Ref &other) const;
            FdIdx FdPair() const;
            LruFD *Get() const;

        private:
            void Clear();
            LruFD *fd_ = nullptr;
            IouringMgr *io_mgr_ = nullptr;
        };

        LruFD(PartitionFiles *tbl, FileId file_id);
        FdIdx FdPair() const;
        void Deque();
        LruFD *DequeNext();
        LruFD *DequePrev();
        void EnqueNext(LruFD *new_fd);

        static constexpr FileId kDirectory = MaxFileId;
        static constexpr FileId kManifest = kDirectory - 1;
        static constexpr FileId kMaxDataFile = kManifest - 1;

        static constexpr int FdEmpty = -1;

        /**
         * @brief mu_ avoids open/close file concurrently.
         */
        Mutex mu_;
        int fd_{FdEmpty};
        int reg_idx_{-1};
        bool dirty_{false};

        PartitionFiles *const tbl_;
        const FileId file_id_;
        uint32_t ref_count_{0};
        LruFD *prev_{nullptr};
        LruFD *next_{nullptr};
    };

    enum class UserDataType : uint8_t
    {
        KvTask,
        BaseReq,
        WriteReq
    };

    struct BaseReq
    {
        explicit BaseReq(KvTask *task = nullptr) : task_(task) {};
        KvTask *task_;
        int res_{0};
        uint32_t flags_{0};
    };

    struct WriteReq
    {
        char *PagePtr() const;
        void SetPage(VarPage page);
        VarPage page_;
        LruFD::Ref fd_ref_;
        WriteTask *task_{nullptr};
        WriteReq *next_{nullptr};
    };

    class PartitionFiles
    {
    public:
        const TableIdent *tbl_id_ = nullptr;
        std::unordered_map<FileId, LruFD> fds_;
    };

    class Manifest : public ManifestFile
    {
    public:
        Manifest(IouringMgr *io_mgr, LruFD::Ref fd, uint64_t size);
        ~Manifest();
        KvError Read(char *dst, size_t n) override;

    private:
        static constexpr uint32_t buf_size = 1 << 20;
        IouringMgr *io_mgr_;
        LruFD::Ref fd_;
        uint64_t file_size_;
        uint64_t file_offset_{0};
        std::unique_ptr<char, decltype(&std::free)> buf_{nullptr, &std::free};
        uint32_t buf_end_{0};
        uint32_t buf_offset_{0};
    };

    static std::pair<void *, UserDataType> DecodeUserData(uint64_t user_data);
    static void EncodeUserData(io_uring_sqe *sqe,
                               const void *ptr,
                               UserDataType type);
    /**
     * @brief Convert file page id to <file_id, file_offset>
     */
    std::pair<FileId, uint32_t> ConvFilePageId(FilePageId file_page_id) const;

    uint32_t AllocRegisterIndex();
    void FreeRegisterIndex(uint32_t idx);

    // Low-level io operation. Very simple wrap on syscall.
    io_uring_sqe *GetSQE(UserDataType type, const void *user_ptr);
    int MakeDir(FdIdx dir_fd, const char *path);
    int OpenAt(FdIdx dir_fd,
               const char *path,
               uint64_t flags,
               uint64_t mode = 0);
    int Read(FdIdx fd, char *dst, size_t n, uint64_t offset);
    int Write(FdIdx fd, const char *src, size_t n, uint64_t offset);
    int Fdatasync(FdIdx fd);
    int Statx(int fd, const char *path, struct statx *result);
    int Rename(FdIdx dir_fd, const char *old_path, const char *new_path);
    int Close(int fd);
    int RegisterFile(int fd);
    int UnregisterFile(int idx);
    int Fallocate(FdIdx fd, uint64_t size);
    int UnlinkAt(FdIdx dir_fd, const char *path, bool rmdir);
    Page SwapPage(Page page, uint16_t buf_id);

    /**
     * @brief Write content to a file with given name in the directory.
     * This is often used to write snapshot of manifest atomically.
     */
    virtual int WriteSnapshot(LruFD::Ref dir_fd,
                              std::string_view name,
                              std::string_view content);
    virtual int CreateFile(LruFD::Ref dir_fd, FileId file_id);
    virtual int OpenFile(const TableIdent &tbl_id, FileId file_id, bool direct);
    virtual KvError SyncFile(LruFD::Ref fd);
    virtual KvError SyncFiles(const TableIdent &tbl_id,
                              std::span<LruFD::Ref> fds);
    virtual KvError CloseFile(LruFD::Ref fd_ref);

    static FdIdx GetRootFD(const TableIdent &tbl_id);
    /**
     * @brief Get file descripter if it is already opened.
     */
    LruFD::Ref GetOpenedFD(const TableIdent &tbl_id, FileId file_id);
    /**
     * @brief Open file if already exists. Only data file is opened with
     * O_DIRECT by default. Set `direct` to true to open manifest with O_DIRECT.
     */
    std::pair<LruFD::Ref, KvError> OpenFD(const TableIdent &tbl_id,
                                          FileId file_id,
                                          bool direct = false);
    /**
     * @brief Open file or create it if not exists. This method can be used to
     * open data-file/manifest or create data-file, but not create manifest.
     * Only data file is opened with O_DIRECT by default. Set `direct` to true
     * to open manifest with O_DIRECT.
     */
    std::pair<LruFD::Ref, KvError> OpenOrCreateFD(const TableIdent &tbl_id,
                                                  FileId file_id,
                                                  bool direct = false,
                                                  bool create = true);
    bool EvictFD();

    class WriteReqPool
    {
    public:
        WriteReqPool(uint32_t pool_size);
        WriteReq *Alloc(LruFD::Ref fd, VarPage page);
        void Free(WriteReq *req);

    private:
        std::unique_ptr<WriteReq[]> pool_;
        WriteReq *free_list_;
        WaitingZone waiting_;
    };

    /**
     * @brief This is only used in non-append mode.
     */
    std::unique_ptr<WriteReqPool> write_req_pool_{nullptr};

    std::unordered_map<TableIdent, PartitionFiles> tables_;
    LruFD lru_fd_head_{nullptr, MaxFileId};
    LruFD lru_fd_tail_{nullptr, MaxFileId};
    uint32_t lru_fd_count_{0};
    const uint32_t fd_limit_;

    uint32_t alloc_reg_slot_{0};
    std::vector<uint32_t> free_reg_slots_;

    io_uring_buf_ring *buf_ring_{nullptr};
    std::vector<Page> bufs_pool_;
    const int buf_group_{0};

    io_uring ring_;
    WaitingZone waiting_sqe_;
    uint32_t prepared_sqe_{0};
};

class CloudStoreMgr : public IouringMgr
{
public:
    CloudStoreMgr(const KvOptions *opts, uint32_t fd_limit);
    static constexpr FileId ManifestFileId()
    {
        return LruFD::kManifest;
    }
    void Start() override;
    bool IsIdle() override;
    void Stop() override;
    void Submit() override;
    void PollComplete() override;
    KvError SwitchManifest(const TableIdent &tbl_id,
                           std::string_view snapshot) override;
    KvError CreateArchive(const TableIdent &tbl_id,
                          std::string_view snapshot,
                          uint64_t ts) override;

    ObjectStore &GetObjectStore()
    {
        return obj_store_;
    }

    KvError ReadArchiveFileAndDelete(const std::string &file_path,
                                     std::string &content);

    bool NeedPrewarm() const override;
    void RunPrewarm() override;
    size_t LocalCacheRemained() const
    {
        return shard_local_space_limit_ - used_local_space_;
    }

private:
    int CreateFile(LruFD::Ref dir_fd, FileId file_id) override;
    int OpenFile(const TableIdent &tbl_id,
                 FileId file_id,
                 bool direct) override;
    KvError SyncFile(LruFD::Ref fd) override;
    KvError SyncFiles(const TableIdent &tbl_id,
                      std::span<LruFD::Ref> fds) override;
    KvError CloseFile(LruFD::Ref fd) override;

    KvError DownloadFile(const TableIdent &tbl_id, FileId file_id);
    KvError UploadFiles(const TableIdent &tbl_id,
                        std::vector<std::string> filenames);

    bool DequeClosedFile(const FileKey &key);
    void EnqueClosedFile(FileKey key);
    bool HasEvictableFile() const;
    int ReserveCacheSpace(size_t size);
    static std::string ToFilename(FileId file_id);
    size_t EstimateFileSize(FileId file_id) const;
    size_t EstimateFileSize(std::string_view filename) const;

    struct CachedFile
    {
        CachedFile() = default;
        const FileKey *key_;
        bool evicting_{false};
        WaitingSeat waiting_;

        void Deque()
        {
            prev_->next_ = next_;
            next_->prev_ = prev_;
            prev_ = nullptr;
            next_ = nullptr;
        }
        void EnqueNext(CachedFile *node)
        {
            node->next_ = next_;
            node->next_->prev_ = node;
            next_ = node;
            node->prev_ = this;
        }
        CachedFile *prev_{nullptr};
        CachedFile *next_{nullptr};
    };

    /**
     * @brief Locally cached files that are not currently opened.
     */
    std::unordered_map<FileKey, CachedFile> closed_files_;
    CachedFile lru_file_head_;
    CachedFile lru_file_tail_;
    size_t used_local_space_{0};
    size_t shard_local_space_limit_{0};

    /**
     * @brief A background task to evict cached files when local space is full.
     */
    class FileCleaner : public KvTask
    {
    public:
        explicit FileCleaner(CloudStoreMgr *io_mgr) : io_mgr_(io_mgr) {};
        TaskType Type() const override;
        void Run();
        void Shutdown();

        WaitingZone requesting_;

    private:
        CloudStoreMgr *io_mgr_;
        bool killed_{false};
    };

    FileCleaner file_cleaner_;
    PrewarmTask prewarm_task_;

    ObjectStore obj_store_;

    friend class PrewarmTask;
    friend class PrewarmService;
};

class MemStoreMgr : public AsyncIoManager
{
public:
    MemStoreMgr(const KvOptions *opts);
    KvError Init(Shard *shard) override;
    void Submit() override {};
    void PollComplete() override {};

    std::pair<Page, KvError> ReadPage(const TableIdent &tbl_id,
                                      FilePageId file_page_id,
                                      Page page) override;
    KvError ReadPages(const TableIdent &tbl_id,
                      std::span<FilePageId> page_ids,
                      std::vector<Page> &pages) override;

    KvError WritePage(const TableIdent &tbl_id,
                      VarPage page,
                      FilePageId file_page_id) override;
    KvError WritePages(const TableIdent &tbl_id,
                       std::span<VarPage> pages,
                       FilePageId first_fp_id) override;
    KvError SyncData(const TableIdent &tbl_id) override;
    KvError AbortWrite(const TableIdent &tbl_id) override;

    KvError AppendManifest(const TableIdent &tbl_id,
                           std::string_view log,
                           uint64_t manifest_size) override;
    KvError SwitchManifest(const TableIdent &tbl_id,
                           std::string_view snapshot) override;
    KvError CreateArchive(const TableIdent &tbl_id,
                          std::string_view snapshot,
                          uint64_t ts) override;
    std::pair<ManifestFilePtr, KvError> GetManifest(
        const TableIdent &tbl_id) override;

    void CleanManifest(const TableIdent &tbl_id) override;

    class Manifest : public ManifestFile
    {
    public:
        explicit Manifest(std::string_view content) : content_(content) {};
        KvError Read(char *dst, size_t n) override;
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

}  // namespace eloqstore
