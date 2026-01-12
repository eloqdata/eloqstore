#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "async_io_manager.h"
#include "compression.h"
#include "error.h"
#include "kv_options.h"
#include "storage/dict_meta.h"
#include "types.h"

namespace eloqstore
{
class DictCache
{
public:
    class Ref
    {
    public:
        Ref() = default;
        Ref(const Ref &other);
        Ref(Ref &&other) noexcept;
        Ref &operator=(const Ref &other);
        Ref &operator=(Ref &&other) noexcept;
        ~Ref();

        explicit operator bool() const
        {
            return entry_ != nullptr;
        }

        compression::DictCompression *Get() const;

    private:
        friend class DictCache;
        Ref(struct Entry *entry, DictCache *owner);
        void Clear();

        struct Entry *entry_{nullptr};
        DictCache *owner_{nullptr};
    };

    DictCache(AsyncIoManager *io_mgr, const KvOptions *options);

    std::pair<Ref, KvError> Acquire(const TableIdent &tbl_id,
                                    const DictMeta &meta);
    void UpdateDictionary(const Ref &ref, const DictMeta &meta);

    size_t UsedBytes() const
    {
        return used_bytes_;
    }

    size_t CapacityBytes() const
    {
        return capacity_bytes_;
    }

    bool Contains(const TableIdent &tbl_id) const
    {
        return entries_.find(tbl_id) != entries_.end();
    }

private:
    struct Entry
    {
        TableIdent tbl_id_;
        DictMeta meta_;
        compression::DictCompression compression_;
        uint32_t ref_count_{0};
        size_t bytes_{0};
        bool in_lru_{false};
        Entry *prev_{nullptr};
        Entry *next_{nullptr};
    };

    Entry *GetEntry(const TableIdent &tbl_id, const DictMeta &meta);
    void Pin(Entry *entry);
    void Unpin(Entry *entry);
    void EnqueueFront(Entry *entry);
    void Dequeue(Entry *entry);
    void EvictIfNeeded();
    KvError LoadDictionary(Entry *entry);

    AsyncIoManager *io_mgr_;
    const KvOptions *options_;
    size_t capacity_bytes_{0};
    size_t used_bytes_{0};
    std::unordered_map<TableIdent, Entry> entries_;
    Entry lru_head_{};
    Entry lru_tail_{};
};
}  // namespace eloqstore
