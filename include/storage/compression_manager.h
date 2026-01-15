#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>

#include "async_io_manager.h"
#include "compression.h"
#include "error.h"
#include "kv_options.h"
#include "types.h"

namespace eloqstore
{
class CompressionManager
{
public:
    struct Entry
    {
        TableIdent tbl_id_{};
        DictMeta meta_{};
        std::shared_ptr<compression::DictCompression> compression_{};
        uint32_t ref_count_{0};
        size_t bytes_{0};
        bool in_lru_{false};
        Entry *prev_{nullptr};
        Entry *next_{nullptr};
    };

    class Handle
    {
    public:
        Handle() = default;
        Handle(const Handle &other);
        Handle(Handle &&other) noexcept;
        Handle &operator=(const Handle &other);
        Handle &operator=(Handle &&other) noexcept;
        ~Handle();

        explicit operator bool() const
        {
            return entry_ != nullptr;
        }

        const std::shared_ptr<compression::DictCompression> &operator->() const
        {
            return compression_;
        }

        const std::shared_ptr<compression::DictCompression> &Shared() const
        {
            return compression_;
        }

    private:
        friend class CompressionManager;
        Handle(struct Entry *entry, CompressionManager *owner);
        void Clear();

        // Keep a pinned cache entry and an owning ref to the dictionary.
        struct Entry *entry_{nullptr};
        CompressionManager *owner_{nullptr};
        // Keep a shared ref to the dictionary to pin it in memory.
        std::shared_ptr<compression::DictCompression> compression_{};
    };

    CompressionManager(AsyncIoManager *io_mgr,
                       const KvOptions *options,
                       size_t capacity_bytes);

    // ManifestFile *manifest is used to load the dictionary in FindRoot
    // function.
    std::pair<Handle, KvError> GetOrLoad(const TableIdent &tbl_id,
                                         const DictMeta &meta,
                                         ManifestFile *manifest = nullptr);
    std::pair<Handle, KvError> GetOrLoadFromBytes(const TableIdent &tbl_id,
                                                  const DictMeta &meta,
                                                  std::string_view dict_bytes);
    void UpdateDictionary(
        const TableIdent &tbl_id,
        const std::shared_ptr<compression::DictCompression> &compression,
        const DictMeta &meta);

    size_t UsedBytes() const
    {
        return used_bytes_;
    }

    size_t CapacityBytes() const
    {
        return capacity_bytes_;
    }

private:
    Entry *GetEntry(const TableIdent &tbl_id, const DictMeta &meta);
    void Pin(Entry *entry);
    void Unpin(Entry *entry);
    void EnqueueFront(Entry *entry);
    void Dequeue(Entry *entry);
    void EvictIfNeeded();
    KvError LoadDictionary(Entry *entry, ManifestFile *manifest);

    AsyncIoManager *io_mgr_;
    const KvOptions *options_;
    size_t capacity_bytes_{0};
    size_t used_bytes_{0};
    std::unordered_map<TableIdent, Entry> entries_;
    Entry lru_head_{};
    Entry lru_tail_{};
};
}  // namespace eloqstore
