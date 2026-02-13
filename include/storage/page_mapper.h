#pragma once

#include <array>
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "pool.h"
#include "storage/mem_index_page.h"
#include "tasks/task.h"
#include "types.h"

namespace eloqstore
{
class IndexPageManager;
class MemIndexPage;
class ManifestBuilder;
class ManifestBuffer;
struct KvOptions;

class MappingArena;
class MappingChunkArena;

struct MappingSnapshot
{
    class Ref
    {
    public:
        explicit Ref(MappingSnapshot *mapping = nullptr);
        Ref(const Ref &other);
        Ref(Ref &&other) noexcept;
        Ref &operator=(const Ref &other);
        Ref &operator=(Ref &&other) noexcept;
        ~Ref();

        MappingSnapshot *Get() const;
        MappingSnapshot *operator->() const;
        friend bool operator==(const Ref &lhs, std::nullptr_t)
        {
            return lhs.mapping_ == nullptr;
        }
        friend bool operator!=(const Ref &lhs, std::nullptr_t)
        {
            return lhs.mapping_ != nullptr;
        }
        explicit operator bool() const
        {
            return mapping_ != nullptr;
        }

    private:
        void Clear();
        MappingSnapshot *mapping_{nullptr};
    };
    class MappingTbl
    {
    public:
        MappingTbl();
        MappingTbl(MappingArena *vector_arena, MappingChunkArena *chunk_arena);
        ~MappingTbl();
        MappingTbl(MappingTbl &&) = default;
        MappingTbl &operator=(MappingTbl &&) = default;
        MappingTbl(const MappingTbl &) = delete;
        MappingTbl &operator=(const MappingTbl &) = delete;

        void clear();
        void reserve(size_t n);
        size_t size() const;
        size_t capacity() const;
        void StartCopying();
        void FinishCopying();
        void ApplyChanges();

        void Set(PageId page_id, uint64_t value);
        PageId PushBack(uint64_t value);
        uint64_t Get(PageId page_id) const;
        void CopyFrom(const MappingTbl &src);
        void ApplyPendingTo(MappingTbl &dst) const;
        bool operator==(const MappingTbl &rhs) const;
        bool operator!=(const MappingTbl &rhs) const
        {
            return !(*this == rhs);
        }

    private:
        static constexpr size_t kChunkShift = 9;
        static constexpr size_t kChunkSize = 1ULL << kChunkShift;
        static constexpr size_t kChunkMask = kChunkSize - 1;

    public:
        using Chunk = std::array<uint64_t, kChunkSize>;

    private:
        void EnsureSize(PageId page_id);
        static size_t RequiredChunks(size_t n);
        void EnsureChunkCount(size_t count);
        void ResizeInternal(size_t new_size, bool init_new_chunks = true);
        std::unique_ptr<Chunk> AcquireChunk();
        void ReleaseChunk(std::unique_ptr<Chunk> chunk);

        bool under_copying_{false};
        absl::flat_hash_map<PageId, uint64_t> changes_;
        std::vector<std::unique_ptr<Chunk>> base_;
        size_t logical_size_{0};
        MappingArena *vector_arena_{nullptr};
        MappingChunkArena *chunk_arena_{nullptr};
    };

    MappingSnapshot(IndexPageManager *idx_mgr,
                    const TableIdent *tbl_id,
                    MappingTbl tbl);
    ~MappingSnapshot();

    static constexpr uint8_t TypeBits = 3;
    static constexpr uint8_t TypeMask = (1 << TypeBits) - 1;
    enum class ValType : uint8_t
    {
        SwizzlingPointer = 0,
        FilePageId,
        PageId,
        Invalid = TypeMask
    };
    static constexpr uint64_t InvalidValue = uint64_t(ValType::Invalid);

    FilePageId ToFilePage(PageId page_id) const;
    FilePageId ToFilePage(uint64_t val) const;

    PageId GetNextFree(PageId page_id) const;

    void AddFreeFilePage(FilePageId file_page);
    void ClearFreeFilePage();

    /**
     * @brief Replaces the swizzling pointer with the file page Id.
     *
     * @param page
     */
    void Unswizzling(MemIndexPage *page);
    MemIndexPage::Handle GetSwizzlingHandle(PageId page_id) const;
    void AddSwizzling(PageId page_id, MemIndexPage *idx_page);

    static bool IsSwizzlingPointer(uint64_t val);
    static bool IsFilePageId(uint64_t val);
    static ValType GetValType(uint64_t val);
    static uint64_t EncodeFilePageId(FilePageId file_page_id);
    static uint64_t EncodePageId(PageId page_id);
    static uint64_t DecodeId(uint64_t val);

    void Serialize(ManifestBuffer &dst) const;

    // Destructors often run on non-shard threads (e.g. Stop() callers) where
    // TLS `shard` is invalid, so we rely on idx_mgr_ to reclaim resources. It
    // remains valid until RootMetaMgr::ReleaseMappers() clears snapshots.
    IndexPageManager *idx_mgr_;
    const TableIdent *tbl_ident_;

    /**
     * @brief A list of file pages to be freed in this mapping snapshot.
     * To-be-freed file pages cannot be put back for re-use if someone is using
     * this snapshot.
     *
     */
    std::vector<FilePageId> to_free_file_pages_;
    /**
     * @brief MappingSnapshot should only be freed and it's file pages recycled
     * after the previous MappingSnapshot has been released. This ensures that
     * file pages are safely reused without risk of premature reclamation.
     */
    Ref next_snapshot_{nullptr};
    MappingTbl mapping_tbl_;

    void AddRef();
    void Release();
    size_t RefCount() const;

private:
    size_t ref_cnt_{0};
};

class MappingChunkArena
{
public:
    std::unique_ptr<MappingSnapshot::MappingTbl::Chunk> Acquire();
    void Release(std::unique_ptr<MappingSnapshot::MappingTbl::Chunk> chunk);
    void Extend();

private:
    std::deque<std::unique_ptr<MappingSnapshot::MappingTbl::Chunk>> pool_;
};

inline std::unique_ptr<MappingSnapshot::MappingTbl::Chunk>
MappingChunkArena::Acquire()
{
    if (pool_.empty())
    {
        Extend();
    }
    std::unique_ptr<MappingSnapshot::MappingTbl::Chunk> chunk =
        std::move(pool_.back());
    pool_.pop_back();
    return chunk;
}

inline void MappingChunkArena::Release(
    std::unique_ptr<MappingSnapshot::MappingTbl::Chunk> chunk)
{
    if (!chunk)
    {
        return;
    }
    pool_.push_back(std::move(chunk));
}

inline void MappingChunkArena::Extend()
{
    for (size_t i = 0; i < 1024; ++i)
    {
        pool_.push_back(std::make_unique<MappingSnapshot::MappingTbl::Chunk>());
    }
}

class MappingArena
{
public:
    using ChunkVector =
        std::vector<std::unique_ptr<MappingSnapshot::MappingTbl::Chunk>>;

    ChunkVector Acquire();
    void Release(ChunkVector &&vec);

private:
    Pool<ChunkVector> pool_;
};

inline MappingArena::ChunkVector MappingArena::Acquire()
{
    return pool_.Acquire();
}

inline void MappingArena::Release(ChunkVector &&vec)
{
    pool_.Release(std::move(vec));
}

/**
 * @brief FilePageAllocator is used to allocate file page id.
 */
class FilePageAllocator
{
public:
    static std::unique_ptr<FilePageAllocator> Instance(const KvOptions *opts);

    FilePageAllocator(const KvOptions *opts, FilePageId max_id = 0);
    FilePageAllocator(const FilePageAllocator &rhs) = default;
    virtual ~FilePageAllocator() = default;
    virtual FilePageId Allocate();
    virtual std::unique_ptr<FilePageAllocator> Clone() = 0;

    FileId CurrentFileId() const;
    FilePageId MaxFilePageId() const;
    uint32_t PagesPerFile() const;

protected:
    const uint8_t pages_per_file_shift_;
    /**
     * @brief (max_fp_id_ - 1) is the maximum allocated file page id, so
     * max_fp_id_ is the smallest unallocated file page id.
     */
    FilePageId max_fp_id_;
};

/**
 * @brief AppendAllocator is used to allocate file page id in append mode.
 * The file page id is allocated in a sequential manner. The file page id is
 * divided into files. Each file contains a fixed number of pages. The file id
 * is the quotient of the file page id divided by the number of pages per file.
 */
class AppendAllocator : public FilePageAllocator
{
public:
    AppendAllocator(const KvOptions *opts)
        : FilePageAllocator(opts, 0), min_file_id_(0), empty_file_cnt_(0) {};
    AppendAllocator(const KvOptions *opts,
                    FileId min_file_id,
                    FilePageId max_fp_id,
                    uint32_t empty_cnt)
        : FilePageAllocator(opts, max_fp_id),
          min_file_id_(min_file_id),
          empty_file_cnt_(empty_cnt) {};
    AppendAllocator(const AppendAllocator &rhs) = default;
    std::unique_ptr<FilePageAllocator> Clone() override;

    void UpdateStat(FileId min_file_id, uint32_t hole_cnt);
    FileId MinFileId() const;

    /**
     * @brief Calculates number of pages this allocator occupied.
     * This result includes pages that is not actually used by mapping but
     * belong to a file used by mapping.
     */
    size_t SpaceSize() const;

private:
    /**
     * @brief The oldest file that is not empty.
     * This is a statistic for calculating space size.
     */
    FileId min_file_id_;
    /**
     * @brief The number of empty file newer than min_file_id_.
     * This is a statistic for calculating space size.
     */
    uint32_t empty_file_cnt_;
};

/**
 * @brief PooledFilePages is used to allocate file page id in pooled mode.
 * The file page id is allocated in a random manner.
 */
class PooledFilePages : public FilePageAllocator
{
public:
    explicit PooledFilePages(const KvOptions *opts)
        : FilePageAllocator(opts) {};
    PooledFilePages(const KvOptions *opts,
                    FilePageId next_id,
                    std::vector<uint32_t> free_ids)
        : FilePageAllocator(opts, next_id), free_ids_(std::move(free_ids)) {};
    PooledFilePages(const PooledFilePages &rhs) = default;
    std::unique_ptr<FilePageAllocator> Clone() override;

    FilePageId Allocate() override;
    void Free(std::vector<FilePageId> fp_ids);

private:
    /**
     * @brief A list of free file page ids.
     * uint32_t is enough to store FilePageId because ids will be reused.
     */
    std::vector<uint32_t> free_ids_;
};

class PageMapper
{
public:
    explicit PageMapper(MappingSnapshot::Ref mapping)
        : mapping_(std::move(mapping)) {};
    PageMapper(IndexPageManager *idx_mgr, const TableIdent *tbl_ident);
    PageMapper(const PageMapper &rhs);

    PageId GetPage();
    void FreePage(PageId page_id);
    FilePageAllocator *FilePgAllocator() const;

    /**
     * @brief Returns the number of valid mapping. Every mapping is a pair of
     * logical page id and file page id.
     */
    uint32_t MappingCount() const;

    MappingSnapshot::Ref GetMappingSnapshot() const;
    MappingSnapshot *GetMapping() const;
    void UpdateMapping(PageId page_id, FilePageId file_page_id);
    uint32_t UseCount() const;
#ifndef NDEBUG
    bool DebugStat() const;
#endif
private:
    const KvOptions *Options() const;
    MappingSnapshot::MappingTbl &Mapping();

    MappingSnapshot::Ref mapping_;
    PageId free_page_head_{MaxPageId};
    uint32_t free_page_cnt_{0};
    std::unique_ptr<FilePageAllocator> file_page_allocator_{nullptr};

    friend class Replayer;
};

}  // namespace eloqstore
