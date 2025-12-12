#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "types.h"

namespace eloqstore
{
class IndexPageManager;
class MemIndexPage;
class ManifestBuilder;
struct KvOptions;

class MappingArena
{
public:
    explicit MappingArena(size_t max_cached = 32) : max_cached_(max_cached)
    {
    }

    std::vector<uint64_t> Get();
    void Return(std::vector<uint64_t> tbl);

private:
    const size_t max_cached_;
    std::vector<std::vector<uint64_t>> pool_;
};

struct MappingSnapshot
{
    MappingSnapshot(IndexPageManager *idx_mgr,
                    const TableIdent *tbl_id,
                    MappingArena *arena = nullptr);
    MappingSnapshot(IndexPageManager *idx_mgr,
                    const TableIdent *tbl_id,
                    std::vector<uint64_t> tbl,
                    MappingArena *arena = nullptr)
        : idx_mgr_(idx_mgr),
          tbl_ident_(tbl_id),
          mapping_tbl_(std::move(tbl)),
          arena_(arena) {};
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
    MemIndexPage *GetSwizzlingPointer(PageId page_id) const;
    void AddSwizzling(PageId page_id, MemIndexPage *idx_page);

    static bool IsSwizzlingPointer(uint64_t val);
    static bool IsFilePageId(uint64_t val);
    static ValType GetValType(uint64_t val);
    static uint64_t EncodeFilePageId(FilePageId file_page_id);
    static uint64_t EncodePageId(PageId page_id);
    static uint64_t DecodeId(uint64_t val);

    void Serialize(std::string &dst) const;

    IndexPageManager *idx_mgr_;
    const TableIdent *tbl_ident_;

    std::vector<uint64_t> mapping_tbl_;
    MappingArena *arena_{nullptr};

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
    std::shared_ptr<MappingSnapshot> next_snapshot_{nullptr};
};

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
    explicit PageMapper(std::shared_ptr<MappingSnapshot> mapping)
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

    std::shared_ptr<MappingSnapshot> GetMappingSnapshot() const;
    MappingSnapshot *GetMapping() const;
    void UpdateMapping(PageId page_id, FilePageId file_page_id);
    uint32_t UseCount();
#ifndef NDEBUG
    bool DebugStat() const;
#endif
private:
    const KvOptions *Options() const;
    std::vector<uint64_t> &Mapping();

    std::shared_ptr<MappingSnapshot> mapping_;
    PageId free_page_head_{MaxPageId};
    uint32_t free_page_cnt_{0};
    std::unique_ptr<FilePageAllocator> file_page_allocator_{nullptr};

    friend class Replayer;
};

}  // namespace eloqstore
