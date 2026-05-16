#pragma once

#include <glog/logging.h>
#include <liburing.h>
#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/node_hash_set.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE

#include "concurrentqueue/concurrentqueue.h"
#include "direct_io_buffer.h"
#include "error.h"
#include "storage/mem_index_page.h"
#include "storage/object_store.h"
#include "tasks/prewarm_task.h"
#include "tasks/task.h"
#include "types.h"

namespace eloqstore
{
class WriteReq;
class WriteTask;
class MemIndexPage;
class GlobalRegisteredMemory;
class CloudStorageService;
class Shard;

class ManifestFile
{
public:
    virtual ~ManifestFile() = default;
    virtual KvError Read(char *dst, size_t n) = 0;
    virtual KvError SkipPadding(size_t n) = 0;
};

using ManifestFilePtr = std::unique_ptr<ManifestFile>;

// TODO(zhanghao): consider using inheritance instead of variant
using VarPage =
    std::variant<MemIndexPage::Handle, DataPage, OverflowPage, Page>;
char *VarPagePtr(const VarPage &page);
enum class VarPageType : uint8_t
{
    MemIndexPage = 0,
    DataPage,
    OverflowPage,
    Page
};

class EloqStore;

// Archive floors for both data and segment files. A file whose id is below the
// corresponding floor is referenced by a retained archive snapshot and must
// not be garbage-collected.
struct ArchivedMaxFileIds
{
    FileId data_file_id{0};
    FileId segment_file_id{0};
};

class AsyncIoManager
{
public:
    explicit AsyncIoManager(const KvOptions *opts) : options_(opts)
    {
        if (options_ != nullptr)
        {
            DirectIoBuffer::UpdateDefaultReserve(options_->DataFileSize());
        }
    };
    virtual ~AsyncIoManager() = default;
    static std::unique_ptr<AsyncIoManager> Instance(const EloqStore *store,
                                                    uint32_t fd_limit);

    /** These methods are provided for worker thread. */
    virtual KvError Init(Shard *shard) = 0;
    virtual bool IsIdle();
    virtual void Stop()
    {
    }
    virtual void NotifyStoreStopping()
    {
        store_stopping_.store(true, std::memory_order_release);
    }
    bool IsStoreStopping() const
    {
        return store_stopping_.load(std::memory_order_acquire);
    }
    virtual void Submit() = 0;
    virtual void PollComplete() = 0;
    virtual bool NeedPrewarm() const
    {
        return false;
    }
    virtual void InitBackgroundJob()
    {
    }
    virtual KvError RestoreStartupState()
    {
        return KvError::NoError;
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

    virtual GlobalRegisteredMemory *GetGlobalRegisteredMemory() const
    {
        return nullptr;
    }
    virtual uint16_t GlobalRegMemIndexBase() const
    {
        return 0;
    }
    /**
     * @brief Resolve a pinned-memory address to its io_uring fixed-buffer
     * index. Returns UINT16_MAX when @p ptr is not within any registered
     * pinned chunk (the default for managers that do not support KV Cache
     * pinned mode).
     */
    virtual uint16_t BufIndexForAddress(const char *ptr) const
    {
        (void) ptr;
        return std::numeric_limits<uint16_t>::max();
    }

    /**
     * @brief Pinned chunk metadata: the chunk's [base, base+size) range and
     * its io_uring fixed-buffer index. Returned by PinnedChunkFor so the
     * caller can both index a fixed write and bounds-check additional
     * sub-ranges (e.g. the rounded-up K*segment_size tail) without a second
     * linear scan.
     */
    struct PinnedChunkInfo
    {
        const char *base;
        size_t size;
        uint16_t buf_index;
    };

    /**
     * @brief Locate the registered pinned chunk containing @p ptr and return
     * its full bounds + buf_index. Returns nullopt when @p ptr is not in any
     * registered chunk.
     */
    virtual std::optional<PinnedChunkInfo> PinnedChunkFor(const char *ptr) const
    {
        (void) ptr;
        return std::nullopt;
    }

    /**
     * @brief Acquire a `segment_size`-sized registered scratch slot, used
     * for the pinned-write tail fallback when the caller's pinned sub-range
     * doesn't extend to K*segment_size. Blocks (yields the coroutine) until
     * a slot is free. Returns nullptr when the pool isn't allocated (non-
     * pinned mode or `pinned_tail_scratch_slots == 0`).
     */
    virtual char *AcquireTailScratch(uint16_t &buf_index)
    {
        (void) buf_index;
        return nullptr;
    }
    virtual void ReleaseTailScratch(char *ptr)
    {
        (void) ptr;
    }
    /**
     * @brief Number of times `AcquireTailScratch` has been called since
     * BootstrapRing. Test-only observability for the pinned-write fast-vs-
     * fallback path; default zero for non-pinned-mode managers.
     */
    virtual size_t TailScratchAcquireCount() const
    {
        return 0;
    }

    /**
     * @brief Read K segments into pre-registered buffers.
     *
     * Every segment except possibly the last reads `segment_size` bytes.
     * The last segment (`segment_ids.back()`) reads `tail_size` bytes when
     * @p tail_size is non-zero; otherwise it also reads `segment_size`.
     * Only the tail may be partial -- the design contract guarantees that
     * non-tail segments in a value are always full.
     *
     * @param tail_size 0 (default) means "all segments read segment_size"
     *   -- preserves existing behavior for callers that don't need partial
     *   reads. When non-zero, must be 4 KiB aligned and `<= segment_size`.
     */
    virtual KvError ReadSegments(const TableIdent &tbl_id,
                                 std::span<const FilePageId> segment_ids,
                                 std::span<char *> dst_ptrs,
                                 std::span<const uint16_t> buf_indices,
                                 uint32_t tail_size = 0)
    {
        (void) tbl_id;
        (void) segment_ids;
        (void) dst_ptrs;
        (void) buf_indices;
        (void) tail_size;
        return KvError::InvalidArgs;
    }
    virtual KvError WriteSegments(const TableIdent &tbl_id,
                                  std::span<const FilePageId> segment_ids,
                                  std::span<const char *> src_ptrs,
                                  std::span<const uint16_t> buf_indices)
    {
        (void) tbl_id;
        (void) segment_ids;
        (void) src_ptrs;
        (void) buf_indices;
        return KvError::InvalidArgs;
    }

    virtual KvError SyncData(const TableIdent &tbl_id) = 0;
    virtual KvError AbortWrite(const TableIdent &tbl_id) = 0;

    virtual KvError AppendManifest(const TableIdent &tbl_id,
                                   std::string_view log,
                                   uint64_t offset) = 0;
    virtual KvError SwitchManifest(const TableIdent &tbl_id,
                                   std::string_view snapshot) = 0;
    virtual KvError CreateArchive(const TableIdent &tbl_id,
                                  std::string_view branch_name,
                                  uint64_t term,
                                  std::string_view snapshot,
                                  std::string_view tag) = 0;
    virtual KvError DeleteArchive(const TableIdent &tbl_id,
                                  std::string_view branch_name,
                                  uint64_t term,
                                  std::string_view tag) = 0;
    virtual KvError WriteBranchManifest(const TableIdent &tbl_id,
                                        std::string_view branch_name,
                                        uint64_t term,
                                        std::string_view snapshot) = 0;
    virtual KvError DeleteBranchFiles(const TableIdent &tbl_id,
                                      std::string_view branch_name,
                                      uint64_t term) = 0;
    virtual std::pair<ManifestFilePtr, KvError> GetManifest(
        const TableIdent &tbl_id) = 0;

    virtual KvError ReadFile(const TableIdent &tbl_id,
                             std::string_view filename,
                             DirectIoBuffer &content)
    {
        __builtin_unreachable();
    }

    virtual bool HasCloudBufferPool() const
    {
        return false;
    }
    virtual DirectIoBuffer AcquireCloudBuffer(KvTask *task)
    {
        (void) task;
        return {};
    }
    virtual void ReleaseCloudBuffer(DirectIoBuffer buffer)
    {
        (void) buffer;
    }

    // Append-mode write buffer pool helpers (default no-op).
    virtual char *AcquireWriteBuffer(uint16_t &buf_index)
    {
        (void) buf_index;
        return nullptr;
    }
    virtual void ReleaseWriteBuffer(char *ptr, uint16_t buf_index)
    {
        (void) ptr;
        (void) buf_index;
    }
    virtual size_t WriteBufferSize() const
    {
        return 0;
    }
    virtual bool HasWriteBufferPool() const
    {
        return false;
    }
    virtual bool WriteBufferUseFixed() const
    {
        return false;
    }
    virtual KvError SubmitMergedWrite(const TableIdent &tbl_id,
                                      TypedFileId file_id,
                                      uint64_t offset,
                                      char *buf_ptr,
                                      size_t bytes,
                                      uint16_t buf_index,
                                      std::vector<VarPage> &pages,
                                      std::vector<char *> &release_ptrs,
                                      std::vector<uint16_t> &release_indices,
                                      bool use_fixed)
    {
        (void) tbl_id;
        (void) file_id;
        (void) offset;
        (void) buf_ptr;
        (void) bytes;
        (void) buf_index;
        (void) pages;
        (void) release_ptrs;
        (void) release_indices;
        (void) use_fixed;
        return KvError::InvalidArgs;
    }
    /**
     * @brief Hook for cloud mode to capture file ranges before write submit.
     *
     * The TypedFileId tells the implementation whether the range belongs to a
     * data file, segment file, or manifest, so a single hook can route to the
     * right per-type upload buffer.
     */
    virtual void OnFileRangeWritePrepared(const TableIdent &tbl_id,
                                          TypedFileId file_id,
                                          std::string_view branch_name,
                                          uint64_t term,
                                          uint64_t offset,
                                          std::string_view data)
    {
        (void) tbl_id;
        (void) file_id;
        (void) branch_name;
        (void) term;
        (void) offset;
        (void) data;
    }
    /**
     * @brief Hook for cloud append mode: invoked when the current data or
     * segment file is sealed (the writer switched away from it).
     *
     * The TypedFileId carries the kind (data vs segment) and the on-disk id,
     * so cloud implementations can pick the right filename for upload.
     */
    virtual KvError OnDataFileSealed(const TableIdent &tbl_id,
                                     TypedFileId file_id)
    {
        (void) tbl_id;
        (void) file_id;
        return KvError::NoError;
    }
    /**
     * @brief Get the number of currently open file descriptors.
     * @return Number of open file descriptors, or 0 if not applicable.
     */
    virtual size_t GetOpenFileCount() const
    {
        return 0;  // Default implementation returns 0 for non-IouringMgr
                   // implementations
    }

    /**
     * @brief Get the file descriptor limit for this shard.
     * @return FD limit, or 0 if not applicable.
     */
    virtual size_t GetOpenFileLimit() const
    {
        return 0;  // Default implementation returns 0 for non-IouringMgr
                   // implementations
    }

    /**
     * @brief Get the current size of locally cached files in bytes.
     * @return Total size in bytes, or 0 if not applicable.
     */
    virtual size_t GetLocalSpaceUsed() const
    {
        return 0;  // Default implementation returns 0 for non-CloudStoreMgr
                   // implementations
    }

    /**
     * @brief Get the local space limit for this shard in bytes.
     * @return Local space limit in bytes, or 0 if not applicable.
     */
    virtual size_t GetLocalSpaceLimit() const
    {
        return 0;  // Default implementation returns 0 for non-CloudStoreMgr
                   // implementations
    }

    virtual KvError CleanManifest(const TableIdent &tbl_id) = 0;
    virtual void RegisterDirBusy(const TableIdent &tbl_id)
    {
        (void) tbl_id;
    }
    virtual void UnregisterDirBusy(const TableIdent &tbl_id)
    {
        (void) tbl_id;
    }
    virtual bool HasDirBusy(const TableIdent &tbl_id) const
    {
        (void) tbl_id;
        return false;
    }
    virtual bool IsDirEvicting(const TableIdent &tbl_id) const
    {
        (void) tbl_id;
        return false;
    }

    // Get branch_name and term for a specific TypedFileId in a table in one
    // lookup. The file_id encodes the kind (data vs segment) so the lookup
    // dispatches to the right max_*_file_id field.
    // Returns true if found, false otherwise (branch_name and term unchanged).
    virtual bool GetBranchNameAndTerm(const TableIdent &tbl_id,
                                      TypedFileId file_id,
                                      std::string &branch_name,
                                      uint64_t &term)
    {
        (void) tbl_id;
        (void) file_id;
        (void) branch_name;
        (void) term;
        return false;
    }

    // Update branch and term for a specific TypedFileId in a table.
    // Updates max_file_id (data) or max_segment_file_id (segment) inside
    // the BranchFileMapping entry for (branch, term), inheriting the other
    // field from the previous entry to keep the per-field non-decreasing
    // invariants.
    virtual void SetBranchFileIdTerm(const TableIdent &tbl_id,
                                     TypedFileId file_id,
                                     std::string_view branch_name,
                                     uint64_t term)
    {
        (void) tbl_id;
        (void) file_id;
        (void) branch_name;
        (void) term;
    }

    // Bulk-replace the BranchFileMapping for a table (used on recovery to
    // restore the full file-range history from the manifest).
    virtual void SetBranchFileMapping(const TableIdent &tbl_id,
                                      BranchFileMapping mapping)
    {
        (void) tbl_id;
        (void) mapping;
    }

    // Return the current BranchFileMapping for a table (used on write to
    // persist the full file-range history into the manifest).
    virtual const BranchFileMapping &GetBranchFileMapping(
        const TableIdent &tbl_id)
    {
        static const BranchFileMapping empty{};
        (void) tbl_id;
        return empty;
    }

    virtual uint64_t ProcessTerm() const
    {
        return 0;
    }

    virtual std::string_view GetActiveBranch() const
    {
        return MainBranchName;
    }

    std::atomic<bool> store_stopping_{false};

    virtual void SetActiveBranch(std::string_view branch)
    {
        (void) branch;
    }

    const KvOptions *options_;
    std::unordered_map<TableIdent, ArchivedMaxFileIds>
        least_not_archived_file_ids_;
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
    char *AcquireWriteBuffer(uint16_t &buf_index) override;
    void ReleaseWriteBuffer(char *ptr, uint16_t buf_index) override;
    size_t WriteBufferSize() const override
    {
        return write_buf_size_;
    }
    bool HasWriteBufferPool() const override
    {
        return write_buf_size_ > 0 && write_buf_ != nullptr;
    }
    bool WriteBufferUseFixed() const override
    {
        return write_buf_registered_;
    }
    KvError SubmitMergedWrite(const TableIdent &tbl_id,
                              TypedFileId file_id,
                              uint64_t offset,
                              char *buf_ptr,
                              size_t bytes,
                              uint16_t buf_index,
                              std::vector<VarPage> &pages,
                              std::vector<char *> &release_ptrs,
                              std::vector<uint16_t> &release_indices,
                              bool use_fixed) override;
    void InitBackgroundJob() override;

    std::pair<Page, KvError> ReadPage(const TableIdent &tbl_id,
                                      FilePageId fp_id,
                                      Page page) override;
    KvError ReadPages(const TableIdent &tbl_id,
                      std::span<FilePageId> page_ids,
                      std::vector<Page> &pages) override;

    KvError WritePage(const TableIdent &tbl_id,
                      VarPage page,
                      FilePageId file_page_id) override;

    KvError ReadSegments(const TableIdent &tbl_id,
                         std::span<const FilePageId> segment_ids,
                         std::span<char *> dst_ptrs,
                         std::span<const uint16_t> buf_indices,
                         uint32_t tail_size = 0) override;
    KvError WriteSegments(const TableIdent &tbl_id,
                          std::span<const FilePageId> segment_ids,
                          std::span<const char *> src_ptrs,
                          std::span<const uint16_t> buf_indices) override;

    KvError SyncData(const TableIdent &tbl_id) override;
    KvError AbortWrite(const TableIdent &tbl_id) override;

    KvError AppendManifest(const TableIdent &tbl_id,
                           std::string_view log,
                           uint64_t offset) override;
    KvError SwitchManifest(const TableIdent &tbl_id,
                           std::string_view snapshot) override;
    KvError CreateArchive(const TableIdent &tbl_id,
                          std::string_view branch_name,
                          uint64_t term,
                          std::string_view snapshot,
                          std::string_view tag) override;
    KvError DeleteArchive(const TableIdent &tbl_id,
                          std::string_view branch_name,
                          uint64_t term,
                          std::string_view tag) override;
    KvError WriteBranchManifest(const TableIdent &tbl_id,
                                std::string_view branch_name,
                                uint64_t term,
                                std::string_view snapshot) override;
    KvError DeleteBranchFiles(const TableIdent &tbl_id,
                              std::string_view branch_name,
                              uint64_t term) override;
    std::pair<ManifestFilePtr, KvError> GetManifest(
        const TableIdent &tbl_id) override;

    // Get branch_name and term for a specific TypedFileId in a table in one
    // lookup.
    bool GetBranchNameAndTerm(const TableIdent &tbl_id,
                              TypedFileId file_id,
                              std::string &branch_name,
                              uint64_t &term) override;

    // Update branch and term for a specific TypedFileId in a table.
    void SetBranchFileIdTerm(const TableIdent &tbl_id,
                             TypedFileId file_id,
                             std::string_view branch_name,
                             uint64_t term) override;

    // Bulk-replace the BranchFileMapping for a table.
    void SetBranchFileMapping(const TableIdent &tbl_id,
                              BranchFileMapping mapping) override;

    // Intern a branch name string and return a stable string_view.
    // The returned view remains valid for the lifetime of this IouringMgr.
    std::string_view InternBranchName(std::string_view name);

    // Return the current BranchFileMapping for a table.
    const BranchFileMapping &GetBranchFileMapping(
        const TableIdent &tbl_id) override;

    // Process term management for term-aware file naming.
    // Local mode always returns 0.
    uint64_t ProcessTerm() const override
    {
        return 0;
    }

    void SetActiveBranch(std::string_view branch) override
    {
        active_branch_ = std::string(branch);
    }

    std::string_view GetActiveBranch() const override
    {
        return active_branch_;
    }

    KvError ReadFile(const TableIdent &tbl_id,
                     std::string_view filename,
                     DirectIoBuffer &content) override;
    KvError DeleteFiles(const std::vector<std::string> &file_paths);
    KvError CloseFiles(const TableIdent &tbl_id,
                       const std::span<TypedFileId> file_ids);

    /**
     * @brief Get the number of currently open file descriptors.
     * @return Number of open file descriptors.
     */
    size_t GetOpenFileCount() const override;

    /**
     * @brief Get the file descriptor limit for this shard.
     * @return FD limit for this shard.
     */
    size_t GetOpenFileLimit() const override;

    size_t GetLocalSpaceUsed() const override
    {
        return 0;  // IouringMgr doesn't use local file caching
    }

    size_t GetLocalSpaceLimit() const override
    {
        return 0;  // IouringMgr doesn't use local file caching
    }

    virtual KvError TryCleanupLocalPartitionDir(const TableIdent &tbl_id);
    KvError CleanManifest(const TableIdent &tbl_id) override;

    static constexpr uint64_t oflags_dir = O_DIRECTORY | O_RDONLY;

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
            explicit operator bool() const
            {
                return fd_ != nullptr;
            }
            FdIdx FdPair() const;
            LruFD *Get() const;

        private:
            void Clear();
            LruFD *fd_ = nullptr;
            IouringMgr *io_mgr_ = nullptr;
        };

        LruFD(PartitionFiles *tbl, TypedFileId file_id, uint64_t term = 0);
        FdIdx FdPair() const;
        void Deque();
        void EnqueNext(LruFD *new_fd);

        static constexpr TypedFileId kDirectory{MaxFileId};
        static constexpr TypedFileId kManifest{MaxFileId - 1};
        // Largest raw FileId that can encode to a non-sentinel TypedFileId.
        // DataFileKey/SegmentFileKey shift left by 1, so any FileId
        // <= kMaxDataFile yields an encoded value < kMaxReserved.
        static constexpr FileId kMaxDataFile = (MaxFileId - 1) >> 1;

        static constexpr int FdEmpty = -1;

        /**
         * @brief mu_ avoids open/close file concurrently.
         */
        Mutex mu_;
        int fd_{FdEmpty};
        int reg_idx_{-1};
        bool dirty_{false};

        PartitionFiles *const tbl_;
        const TypedFileId file_id_;
        uint32_t ref_count_{0};
        LruFD *prev_{nullptr};
        LruFD *next_{nullptr};
        uint64_t term_{0};  // Term of the file this FD represents
        std::string_view
            branch_name_;  // Branch name of the file this FD represents
    };

    enum class UserDataType : uint8_t
    {
        KvTask,
        BaseReq,
        WriteReq,
        MergedWriteReq
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

    struct MergedWriteReq
    {
        WriteTask *task_{nullptr};
        LruFD::Ref fd_ref_;
        char *buf_ptr_{nullptr};
        uint16_t buf_index_{0};
        bool use_fixed_{true};
        size_t bytes_{0};
        uint64_t offset_{0};
        std::vector<VarPage> pages_;
        std::vector<char *> release_ptrs_;
        std::vector<uint16_t> release_indices_;
        MergedWriteReq *next_{nullptr};
    };

    class PartitionFiles
    {
    public:
        const TableIdent *tbl_id_ = nullptr;
        std::unordered_map<TypedFileId, LruFD> fds_;
    };

    class Manifest : public ManifestFile
    {
    public:
        Manifest(IouringMgr *io_mgr, LruFD::Ref fd, uint64_t size);
        ~Manifest();
        KvError Read(char *dst, size_t n) override;
        KvError SkipPadding(size_t n) override;

    private:
        KvError EnsureBuffered();
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
    std::pair<FileId, uint32_t> ConvFileSegmentId(
        FilePageId file_segment_id) const;

    uint32_t AllocRegisterIndex();
    void FreeRegisterIndex(uint32_t idx);

    uint16_t LookupRegisteredBufferIndex(const char *ptr) const;

    // Low-level io operation. Very simple wrap on syscall.
    io_uring_sqe *GetSQE(UserDataType type, const void *user_ptr);
    int MakeDir(FdIdx dir_fd, const char *path);
    int OpenAt(FdIdx dir_fd,
               const char *path,
               uint64_t flags,
               uint64_t mode = 0,
               bool fixed_target = true);
    int Read(FdIdx fd, char *dst, size_t n, uint64_t offset);
    int Write(FdIdx fd, const char *src, size_t n, uint64_t offset);
    virtual int Fdatasync(FdIdx fd);
    int Statx(FdIdx fd, const char *path, struct statx *result);
    int StatxAt(FdIdx dir_fd, const char *path, struct statx *result);
    int Rename(FdIdx dir_fd, const char *old_path, const char *new_path);
    int Close(int fd);
    int CloseDirect(int idx);
    int Fallocate(FdIdx fd, uint64_t size);
    int UnlinkAt(FdIdx dir_fd, const char *path, bool rmdir);
    /**
     * @brief Write content to a file with given name in the directory.
     * This is often used to write snapshot of manifest atomically.
     */
    virtual int WriteSnapshot(LruFD::Ref dir_fd,
                              std::string_view name,
                              std::string_view content);
    virtual int CreateFile(LruFD::Ref dir_fd,
                           TypedFileId file_id,
                           std::string_view branch_name,
                           uint64_t term);
    virtual int OpenFile(const TableIdent &tbl_id,
                         TypedFileId file_id,
                         uint64_t flags,
                         uint64_t mode,
                         std::string_view branch_name,
                         uint64_t term,
                         bool skip_cloud_lookup = false);
    virtual void WaitForEvictingPath(const TableIdent &tbl_id,
                                     TypedFileId file_id,
                                     std::string_view branch_name,
                                     uint64_t term)
    {
        (void) tbl_id;
        (void) file_id;
        (void) branch_name;
        (void) term;
    }
    virtual bool StartEvictingPath(const TableIdent &tbl_id,
                                   TypedFileId file_id,
                                   std::string_view branch_name,
                                   uint64_t term)
    {
        (void) tbl_id;
        (void) file_id;
        (void) branch_name;
        (void) term;
        return true;
    }
    virtual void FinishEvictingPath(const TableIdent &tbl_id,
                                    TypedFileId file_id,
                                    std::string_view branch_name,
                                    uint64_t term)
    {
        (void) tbl_id;
        (void) file_id;
        (void) branch_name;
        (void) term;
    }
    virtual KvError SyncFile(LruFD::Ref fd);
    virtual KvError SyncFiles(const TableIdent &tbl_id,
                              std::span<LruFD::Ref> fds);
    KvError CloseFiles(std::span<LruFD::Ref> fds);
    virtual KvError FdatasyncFiles(const TableIdent &tbl_id,
                                   std::span<LruFD::Ref> fds);
    virtual KvError CloseFile(LruFD::Ref fd_ref);
    bool HasOtherFile(const TableIdent &tbl_id) const;

    FdIdx GetRootFD(const TableIdent &tbl_id);
    /**
     * @brief Get file descripter if it is already opened.
     */
    LruFD::Ref GetOpenedFD(const TableIdent &tbl_id, TypedFileId file_id);
    /**
     * @brief Open file if already exists. Only data file is opened with
     * O_DIRECT by default. Set `direct` to true to open manifest with O_DIRECT.
     */
    std::pair<LruFD::Ref, KvError> OpenFD(const TableIdent &tbl_id,
                                          TypedFileId file_id,
                                          bool direct,
                                          std::string_view branch_name,
                                          uint64_t term);
    /**
     * @brief Open file or create it if not exists. This method can be used to
     * open data-file/manifest or create data-file, but not create manifest.
     * Only data file is opened with O_DIRECT by default. Set `direct` to true
     * to open manifest with O_DIRECT. When `skip_cloud_lookup` is true, cloud
     * implementations may return ENOENT directly on local miss and let the
     * caller decide whether to create the file.
     */
    std::pair<LruFD::Ref, KvError> OpenOrCreateFD(
        const TableIdent &tbl_id,
        TypedFileId file_id,
        bool direct,
        bool create,
        std::string_view branch_name,
        uint64_t term,
        bool skip_cloud_lookup = false);
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

    class MergedWriteReqPool
    {
    public:
        explicit MergedWriteReqPool(uint32_t pool_size);
        MergedWriteReq *Alloc(WriteTask *task,
                              LruFD::Ref fd,
                              char *buf_ptr,
                              uint16_t buf_index,
                              size_t bytes,
                              uint64_t offset,
                              std::vector<VarPage> pages);
        void Free(MergedWriteReq *req);

    private:
        std::unique_ptr<MergedWriteReq[]> pool_;
        MergedWriteReq *free_list_;
        WaitingZone waiting_;
    };

    /**
     * @brief This is only used in non-append mode.
     */
    std::unique_ptr<WriteReqPool> write_req_pool_{nullptr};
    std::unique_ptr<MergedWriteReqPool> merged_write_req_pool_{nullptr};

    std::unordered_map<TableIdent, PartitionFiles> tables_;
    // Per-table BranchFileMapping storage (branch_name, term, max_file_id
    // ranges).
    absl::flat_hash_map<TableIdent, BranchFileMapping> branch_file_mapping_;
    LruFD lru_fd_head_{nullptr, TypedFileId{MaxFileId}};
    LruFD lru_fd_tail_{nullptr, TypedFileId{MaxFileId}};
    uint32_t lru_fd_count_{0};
    const uint32_t fd_limit_;

    uint32_t alloc_reg_slot_{0};
    std::vector<uint32_t> free_reg_slots_;

    bool ring_inited_{false};
    bool buffers_registered_{false};
    char *registered_buf_base_{nullptr};
    size_t registered_buf_stride_{0};
    uint8_t registered_buf_shift_{0};
    uint16_t registered_buf_count_{0};
    size_t registered_last_slice_size_{0};
    std::unique_ptr<char, decltype(&std::free)> write_buf_{nullptr, &std::free};
    size_t write_buf_size_{0};
    size_t write_buf_pool_size_{0};
    uint16_t write_buf_count_{0};
    bool write_buf_registered_{false};
    uint16_t write_buf_index_base_{0};
    struct WriteBufSlot
    {
        WriteBufSlot *next{nullptr};
        uint16_t index{0};
    };
    std::vector<WriteBufSlot> write_buf_slots_;
    WriteBufSlot *write_buf_free_{nullptr};
    WaitingZone write_buf_waiting_;

    io_uring ring_;
    WaitingZone waiting_sqe_;
    uint32_t prepared_sqe_{0};

    // Counter for consecutive Submit() calls that skipped the kernel
    // entry (no prepared SQEs and IORING_SQ_TASKRUN not set). When the
    // ring is configured with IORING_SETUP_DEFER_TASKRUN, the kernel
    // never delivers CQEs autonomously -- the user thread must enter
    // via io_uring_enter(GETEVENTS) to drive deferred taskrun. There is
    // a brief window after I/O completion before the kernel sets
    // IORING_SQ_TASKRUN; if the shard polls the flag during that window
    // and there are no SQEs to submit, it would otherwise spin in user
    // mode forever (observed deadlock at high read concurrency). To
    // bound that window, we force an io_uring_enter(GETEVENTS) every
    // kForceSubmitEveryNoOps iterations even when the flag says
    // there's nothing to do.
    static constexpr uint32_t kForceSubmitEveryNoOps = 10;
    uint32_t consecutive_skipped_submits_{0};

    // Active branch for this shard.
    std::string active_branch_{MainBranchName};

    // Pool of interned branch name strings. Provides stable string_view
    // references for LruFD::branch_name_ without per-FD heap allocations.
    // Uses node_hash_set for pointer stability across insertions.
    absl::node_hash_set<std::string> branch_name_pool_;

    /**
     * Bootstrap inputs for the very-large-value zero-copy memory.
     *  - `GlobalRegisteredMemory *`: external instance owned by the caller
     *    (legacy zero-copy mode). nullptr disables zero-copy entirely.
     *  - `std::span<const std::pair<char*, size_t>>`: KV Cache pinned memory
     *    chunks (shared across shards). EloqStore additionally constructs a
     *    private GlobalRegisteredMemory per shard to back background tasks
     *    that cannot use the pinned chunks.
     */
    using GlobalMemoryConfig =
        std::variant<GlobalRegisteredMemory *,
                     std::span<const std::pair<char *, size_t>>>;

    KvError BootstrapRing(Shard *shard, GlobalMemoryConfig config = {});

    // Set when the GlobalRegisteredMemory pointed to by global_reg_mem_ is
    // owned by this IouringMgr (KV Cache pinned-mode private GC pool).
    std::unique_ptr<GlobalRegisteredMemory> private_gc_mem_;
    GlobalRegisteredMemory *global_reg_mem_{nullptr};
    uint16_t global_reg_mem_index_base_{0};

    // KV Cache pinned-mode metadata. Empty when not in pinned mode.
    std::vector<std::pair<char *, size_t>> pinned_chunks_;
    uint16_t pinned_index_base_{0};

    // Tail-scratch pool: a contiguous registered buffer of
    // `pinned_tail_scratch_slots * segment_size` bytes used by the pinned-
    // write tail fallback. Allocated only when the KV Cache pinned mode is
    // active and `pinned_tail_scratch_slots > 0`. The slot size is
    // `options_->segment_size` and the slot count is
    // `options_->pinned_tail_scratch_slots`; both are read directly from
    // KvOptions rather than mirrored as members.
    //
    // Registered as a *single* iovec covering the whole buffer: the kernel
    // only requires that `[slot_ptr, slot_ptr + segment_size)` lies within
    // the iovec at `tail_scratch_buf_idx_`, which is trivially true for any
    // slot in the contiguous buffer. All slots share the same buf_index.
    //
    // The free list is intrusive: when a slot is free, its first 8 bytes
    // hold a `char *` to the next free slot (or nullptr at the tail). The
    // free-state and in-use-state never overlap in time -- once a slot is
    // acquired, the caller overwrites it with I/O data; once released, we
    // immediately re-stamp the first 8 bytes with the next-free pointer. The
    // pool is per-shard and single-threaded (only the shard's coroutine
    // touches it), so the concurrency hazards that forced
    // `GlobalRegisteredMemory` to use a separate `successors_` table do not
    // apply here. The pointer is read/written via `std::memcpy` to sidestep
    // strict-aliasing concerns.
    std::unique_ptr<char, decltype(&std::free)> tail_scratch_buf_{nullptr,
                                                                  &std::free};
    uint16_t tail_scratch_buf_idx_{0};  // iovec index of the pool
    char *tail_scratch_free_{nullptr};  // intrusive free-list head
    WaitingZone tail_scratch_waiting_;
    // Increment-on-acquire counter, exposed for tests that want to assert
    // the fast-path (no-scratch) frequency. Not used by production code.
    size_t tail_scratch_acquire_count_{0};

public:
    GlobalRegisteredMemory *GetGlobalRegisteredMemory() const override
    {
        return global_reg_mem_;
    }
    uint16_t GlobalRegMemIndexBase() const override
    {
        return global_reg_mem_index_base_;
    }
    /**
     * @brief Resolve a pinned-memory address to its io_uring fixed-buffer
     * index. Linear-searches the registered pinned chunks; the address must
     * fall within exactly one chunk. Returns UINT16_MAX if not found.
     */
    uint16_t BufIndexForAddress(const char *ptr) const override;

    char *AcquireTailScratch(uint16_t &buf_index) override;
    void ReleaseTailScratch(char *ptr) override;
    size_t TailScratchAcquireCount() const override
    {
        return tail_scratch_acquire_count_;
    }

    /**
     * @brief Pure-function form of BufIndexForAddress: searches @p chunks for
     * the one containing @p ptr and returns base_index + chunk_index, or
     * UINT16_MAX if not found. Exposed statically so the lookup can be unit-
     * tested without standing up a full IouringMgr.
     */
    static uint16_t LookupBufIndex(
        std::span<const std::pair<char *, size_t>> chunks,
        uint16_t base_index,
        const char *ptr);

    /**
     * @brief Locate the pinned chunk containing @p ptr and return its
     * bounds + buf_index. See AsyncIoManager::PinnedChunkFor.
     */
    std::optional<PinnedChunkInfo> PinnedChunkFor(
        const char *ptr) const override;

    /**
     * @brief Pure-function form of PinnedChunkFor: counterpart to
     * LookupBufIndex that returns the full chunk record instead of just
     * its buf_index.
     */
    static std::optional<PinnedChunkInfo> LookupPinnedChunk(
        std::span<const std::pair<char *, size_t>> chunks,
        uint16_t base_index,
        const char *ptr);
};

class CloudStoreMgr final : public IouringMgr
{
public:
    CloudStoreMgr(const KvOptions *opts,
                  uint32_t fd_limit,
                  CloudStorageService *service);
    ~CloudStoreMgr() override;
    static constexpr TypedFileId ManifestFileId()
    {
        return LruFD::kManifest;
    }
    KvError Init(Shard *shard) override;
    KvError RestoreStartupState() override;
    bool IsIdle() override;
    void Stop() override;
    KvError SwitchManifest(const TableIdent &tbl_id,
                           std::string_view snapshot) override;
    KvError CreateArchive(const TableIdent &tbl_id,
                          std::string_view branch_name,
                          uint64_t term,
                          std::string_view snapshot,
                          std::string_view tag) override;
    KvError DeleteArchive(const TableIdent &tbl_id,
                          std::string_view branch_name,
                          uint64_t term,
                          std::string_view tag) override;
    KvError WriteBranchManifest(const TableIdent &tbl_id,
                                std::string_view branch_name,
                                uint64_t term,
                                std::string_view snapshot) override;
    KvError DeleteBranchFiles(const TableIdent &tbl_id,
                              std::string_view branch_name,
                              uint64_t term) override;
    KvError AbortWrite(const TableIdent &tbl_id) override;
    KvError CleanManifest(const TableIdent &tbl_id) override;

    ObjectStore &GetObjectStore()
    {
        return obj_store_;
    }

    KvError ReadArchiveFileAndDelete(const TableIdent &tbl_id,
                                     const std::string &filename,
                                     DirectIoBuffer &content);

    bool NeedPrewarm() const override;
    void RunPrewarm() override;
    // Writes buffer contents starting at offset within filename.
    KvError WriteFile(const TableIdent &tbl_id,
                      std::string_view filename,
                      const DirectIoBuffer &buffer,
                      uint64_t flags,
                      uint64_t offset = 0);
    size_t LocalCacheRemained() const
    {
        return shard_local_space_limit_ - used_local_space_;
    }
    /**
     * @brief Get the current size of locally cached files in bytes.
     * @return Total size in bytes.
     */
    size_t GetLocalSpaceUsed() const override;

    /**
     * @brief Get the local space limit for this shard in bytes.
     * @return Local space limit in bytes.
     */
    size_t GetLocalSpaceLimit() const override;
    size_t ActivePrewarmTasks() const
    {
        return active_prewarm_tasks_;
    }
    // Cloud mode does not need fsync.
    int Fdatasync(FdIdx fd) override
    {
        return 0;
    }
    KvError FdatasyncFiles(const TableIdent &tbl_id,
                           std::span<LruFD::Ref> fds) override
    {
        return KvError::NoError;
    }
    void RegisterPrewarmActive();
    void UnregisterPrewarmActive();
    bool HasPrewarmPending() const;
    bool PopPrewarmFile(PrewarmFile &file);
    void ClearPrewarmFiles();
    void StopAllPrewarmTasks();
    void AcquireCloudSlot(KvTask *task);
    void ReleaseCloudSlot(size_t count = 1);
    void EnqueueCloudReadyTask(ObjectStore::Task *task);
    void ProcessCloudReadyTasks(Shard *shard);
    bool AppendPrewarmFiles(std::vector<PrewarmFile> &files);
    size_t GetPrewarmPendingCount() const;
    void MarkPrewarmListingComplete();
    bool IsPrewarmListingComplete() const;
    size_t GetPrewarmFilesPulled() const;
    void RecycleBuffers(std::vector<DirectIoBuffer> &buffers);
    void RecycleBuffer(DirectIoBuffer buffer);
    DirectIoBufferPool &GetDirectIoBufferPool();
    bool HasCloudBufferPool() const override
    {
        return true;
    }
    DirectIoBuffer AcquireCloudBuffer(KvTask *task) override;
    void ReleaseCloudBuffer(DirectIoBuffer buffer) override;
    PrewarmStats &GetPrewarmStats()
    {
        return prewarm_stats_;
    }
    const PrewarmStats &GetPrewarmStats() const
    {
        return prewarm_stats_;
    }
    bool HasPrewarmWorkers() const
    {
        return !prewarmers_.empty();
    }

    void SetProcessTerm(uint64_t term)
    {
        process_term_ = term;
    }
    void SetPartitionGroupId(PartitonGroupId partition_group_id)
    {
        partition_group_id_ = partition_group_id;
    }
    PartitonGroupId PartitionGroupId() const
    {
        return partition_group_id_;
    }
    uint64_t ProcessTerm() const override
    {
        return process_term_;
    }
    void OnFileRangeWritePrepared(const TableIdent &tbl_id,
                                  TypedFileId file_id,
                                  std::string_view branch_name,
                                  uint64_t term,
                                  uint64_t offset,
                                  std::string_view data) override;
    // Called when append-mode writing switches away from a data or segment
    // file. Upload success marks that file clean; failure aborts the task.
    KvError OnDataFileSealed(const TableIdent &tbl_id,
                             TypedFileId file_id) override;
    KvError AppendManifest(const TableIdent &tbl_id,
                           std::string_view log,
                           uint64_t offset) override;

    std::pair<ManifestFilePtr, KvError> GetManifest(
        const TableIdent &tbl_id) override;
    std::pair<ManifestFilePtr, KvError> RefreshManifest(
        const TableIdent &tbl_id, std::string_view archive_tag);
    KvError TryCleanupLocalPartitionDir(const TableIdent &tbl_id) override;
    KvError CleanupLocalPartitionFiles(const TableIdent &tbl_id);
    void ScheduleLocalFileCleanup(const TableIdent &tbl_id,
                                  const std::vector<std::string> &filenames);
    void RegisterDirBusy(const TableIdent &tbl_id) override;
    void UnregisterDirBusy(const TableIdent &tbl_id) override;
    bool HasDirBusy(const TableIdent &tbl_id) const override;
    bool IsDirEvicting(const TableIdent &tbl_id) const override;
    // Downloads the cloud file and writes it into the local file from offset.
    KvError DownloadFile(const TableIdent &tbl_id,
                         TypedFileId file_id,
                         std::string_view branch_name,
                         uint64_t term,
                         bool download_to_exist = false,
                         uint64_t offset = 0);
    KvError CloseFile(LruFD::Ref fd) override;

    // Read partition-group CURRENT_TERM file from cloud, returns
    // {term_value, etag, error}. If file doesn't exist (404), returns
    // {0, "", NotFound}.
    std::tuple<uint64_t, std::string, KvError> ReadTermFile(
        std::string_view branch_name);

private:
    void WaitForCloudTasksToDrain();

private:
    int CreateFile(LruFD::Ref dir_fd,
                   TypedFileId file_id,
                   std::string_view branch_name,
                   uint64_t term) override;
    int OpenFile(const TableIdent &tbl_id,
                 TypedFileId file_id,
                 uint64_t flags,
                 uint64_t mode,
                 std::string_view branch_name,
                 uint64_t term = 0,
                 bool skip_cloud_lookup = false) override;
    void WaitForEvictingPath(const TableIdent &tbl_id,
                             TypedFileId file_id,
                             std::string_view branch_name,
                             uint64_t term) override;
    bool StartEvictingPath(const TableIdent &tbl_id,
                           TypedFileId file_id,
                           std::string_view branch_name,
                           uint64_t term) override;
    void FinishEvictingPath(const TableIdent &tbl_id,
                            TypedFileId file_id,
                            std::string_view branch_name,
                            uint64_t term) override;
    KvError SyncFile(LruFD::Ref fd) override;
    KvError SyncFiles(const TableIdent &tbl_id,
                      std::span<LruFD::Ref> fds) override;

    KvError UploadFile(const TableIdent &tbl_id,
                       std::string filename,
                       WriteTask *owner,
                       std::string_view payload = {},
                       bool wait_for_completion = true,
                       FdIdx cached_fd = {-1, false});
    KvError UploadFiles(const TableIdent &tbl_id,
                        std::vector<std::pair<std::string, FdIdx>> files);
    /**
     * @brief Read file prefix from disk for upload fallback.
     *
     * When in-memory buffered bytes only cover a file tail (e.g., recent
     * appends), this reads the file prefix [0, prefix_len) from disk directly
     * into the destination buffer.
     *
     * @param tbl_id Table identifier
     * @param filename File name to read
     * @param prefix_len Number of bytes to read from file start
     * @param buffer Destination buffer
     * @param dst_offset Byte offset in destination buffer to place the prefix
     *
     * @return KvError::NoError on success, error code on failure
     */
    KvError ReadFilePrefix(const TableIdent &tbl_id,
                           std::string_view filename,
                           size_t prefix_len,
                           DirectIoBuffer &buffer,
                           size_t dst_offset);
    /**
     * @brief Read file prefix using a cached FD (inode-based) instead of
     * opening a new FD by path.
     *
     * This avoids a race where a concurrent rename() replaces the file on
     * disk between a write and the subsequent prefix read for upload.  The
     * cached FD references the inode directly, so it is immune to path-level
     * replacement.
     */
    KvError ReadFilePrefix(const TableIdent &tbl_id,
                           std::string_view filename,
                           size_t prefix_len,
                           DirectIoBuffer &buffer,
                           size_t dst_offset,
                           FdIdx cached_fd);

    void IncrementClosedFileCount(const TableIdent &tbl_id);
    void DecrementClosedFileCount(const TableIdent &tbl_id);
    bool HasTrackedLocalFiles(const TableIdent &tbl_id) const;
    bool DequeClosedFile(const FileKey &key);
    void EnqueClosedFile(FileKey key);
    bool HasEvictableFile() const;
    int ReserveCacheSpace(size_t size);
    static std::string ToFilename(TypedFileId file_id, uint64_t term = 0);
    size_t EstimateFileSize(TypedFileId file_id) const;
    size_t EstimateFileSize(std::string_view filename) const;
    void InitBackgroundJob() override;
    KvError RestoreLocalCacheState();
    /**
     * @brief Register pre-existing cached files for one partition on warm
     * start.
     *
     * Walks a single partition directory and registers every cacheable file
     * (data, manifest, or segment) with the closed-file LRU so the shard can
     * serve reads from the local cache instead of re-downloading from the
     * object store. Removes stray `*.tmp` leftovers, rejects unknown file
     * types, and bumps the shard's `used_local_space_`. Invoked once per
     * partition by `RestoreLocalCacheState()` during `Init()` when
     * `allow_reuse_local_caches` is set.
     *
     * @param tbl_id Table identifier for the partition being restored
     * @param table_path Absolute filesystem path to the partition directory
     * @param restored_files In/out counter incremented by the number of files
     * registered
     * @param restored_bytes In/out counter incremented by the total estimated
     * bytes of the registered files
     *
     * @return KvError::NoError on success, error code on failure
     */
    KvError RestoreFilesForTable(const TableIdent &tbl_id,
                                 const fs::path &table_path,
                                 size_t &restored_files,
                                 size_t &restored_bytes);
    std::pair<size_t, size_t> TrimRestoredCacheUsage();
    FileKey EvictingPathKey(const TableIdent &tbl_id,
                            TypedFileId file_id,
                            std::string_view branch_name,
                            uint64_t term) const;
    void WaitForEvictingKey(const FileKey &key);
    bool StartEvictingKey(FileKey key);
    void FinishEvictingKey(const FileKey &key);
    bool IsEvictingKey(const FileKey &key) const;

    struct CachedFile
    {
        CachedFile() = default;
        const FileKey *key_;

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
    struct EvictingPath
    {
        WaitingZone waiting_;
    };
    std::unordered_map<FileKey, EvictingPath> evicting_paths_;
    std::unordered_set<FileKey> pending_gc_cleanup_;
    std::unordered_map<TableIdent, size_t> closed_file_counts_;
    std::deque<TableIdent> pending_dir_cleanup_;
    std::unordered_map<TableIdent, uint32_t> dir_busy_counts_;
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
        explicit FileCleaner(CloudStoreMgr *io_mgr) : io_mgr_(io_mgr)
        {
        }
        TaskType Type() const override;
        void Run();
        void Shutdown();

        WaitingZone requesting_;

    private:
        CloudStoreMgr *io_mgr_;
        bool killed_{true};
    };

    FileCleaner file_cleaner_;
    std::vector<std::unique_ptr<Prewarmer>> prewarmers_;
    size_t active_prewarm_tasks_{0};

    // Prewarm queue management
    moodycamel::ConcurrentQueue<ObjectStore::Task *> cloud_ready_tasks_;
    moodycamel::ConcurrentQueue<PrewarmFile> prewarm_queue_;
    static constexpr size_t kMaxPrewarmPendingFiles = 1000;
    std::atomic<bool> prewarm_listing_complete_{false};
    std::atomic<size_t> prewarm_queue_size_{0};    // Accurate size tracking
    std::atomic<size_t> prewarm_files_pulled_{0};  // Track files consumed

    // Store shard ID for worker notification
    size_t shard_id_{0};

    // Prewarm statistics
    PrewarmStats prewarm_stats_;

    DirectIoBufferPool direct_io_buffer_pool_;
    ObjectStore obj_store_;
    CloudStorageService *cloud_service_{nullptr};

    // Expected process term for this shard in cloud mode.
    // 0 means unspecified/legacy; in that case term validation in GetManifest
    // will be skipped and the latest manifest term will be used.
    uint64_t process_term_{0};
    PartitonGroupId partition_group_id_{0};

    size_t inflight_cloud_slots_{0};
    WaitingZone cloud_slot_waiting_;
    size_t inflight_cloud_buffers_{0};
    WaitingZone cloud_buffer_waiting_;

    friend class Prewarmer;
    friend class PrewarmService;
};

class StandbyStoreMgr final : public IouringMgr
{
public:
    StandbyStoreMgr(const KvOptions *opts, uint32_t fd_limit);
    void Stop() override;
    void SetProcessTerm(uint64_t term)
    {
        process_term_ = term;
    }
    uint64_t ProcessTerm() const override
    {
        return process_term_;
    }

    std::pair<ManifestFilePtr, KvError> GetManifest(
        const TableIdent &tbl_id) override;

    std::pair<ManifestFilePtr, KvError> RefreshManifest(
        const TableIdent &tbl_id);

private:
    void WaitForStandbyTasksToDrain();
    std::string BuildRemoteFilePath(const TableIdent &tbl_id,
                                    std::string_view filename) const;
    int RunRsync(const std::string &remote, const std::string &dst);
    std::atomic<size_t> inflight_standby_tasks_{0};
    uint64_t process_term_{0};

    std::string remote_addr_;
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
    KvError SyncData(const TableIdent &tbl_id) override;
    KvError AbortWrite(const TableIdent &tbl_id) override;

    KvError AppendManifest(const TableIdent &tbl_id,
                           std::string_view log,
                           uint64_t offset) override;
    KvError SwitchManifest(const TableIdent &tbl_id,
                           std::string_view snapshot) override;
    KvError CreateArchive(const TableIdent &tbl_id,
                          std::string_view branch_name,
                          uint64_t term,
                          std::string_view snapshot,
                          std::string_view tag) override;
    KvError DeleteArchive(const TableIdent &tbl_id,
                          std::string_view branch_name,
                          uint64_t term,
                          std::string_view tag) override;
    KvError WriteBranchManifest(const TableIdent &tbl_id,
                                std::string_view branch_name,
                                uint64_t term,
                                std::string_view snapshot) override;
    KvError DeleteBranchFiles(const TableIdent &tbl_id,
                              std::string_view branch_name,
                              uint64_t term) override;
    std::pair<ManifestFilePtr, KvError> GetManifest(
        const TableIdent &tbl_id) override;

    size_t GetOpenFileCount() const override
    {
        return 0;  // MemStoreMgr doesn't use file descriptors
    }

    size_t GetOpenFileLimit() const override
    {
        return 0;  // MemStoreMgr doesn't use file descriptors
    }

    size_t GetLocalSpaceUsed() const override
    {
        return 0;  // MemStoreMgr doesn't use local file caching
    }

    size_t GetLocalSpaceLimit() const override
    {
        return 0;  // MemStoreMgr doesn't use local file caching
    }

    KvError CleanManifest(const TableIdent &tbl_id) override;

    class Manifest : public ManifestFile
    {
    public:
        explicit Manifest(std::string_view content) : content_(content) {};
        KvError Read(char *dst, size_t n) override;
        KvError SkipPadding(size_t n) override;

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
    std::unordered_map<TableIdent, std::unordered_map<std::string, std::string>>
        manifests_;
    std::mutex manifest_mutex_;
};

}  // namespace eloqstore
