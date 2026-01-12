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
#include "storage/dict_meta.h"
#include "types.h"

namespace eloqstore
{
class CompressionManager
{
public:
    struct Key
    {
        TableIdent tbl_id_;
        uint64_t dict_epoch_{0};

        bool operator==(const Key &other) const
        {
            return tbl_id_ == other.tbl_id_ &&
                   dict_epoch_ == other.dict_epoch_;
        }
    };

    struct Entry
    {
        Key key_{};
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

        compression::DictCompression *Get() const;
        std::shared_ptr<compression::DictCompression> Shared() const;

    private:
        friend class CompressionManager;
        Handle(struct Entry *entry, CompressionManager *owner);
        void Clear();

        struct Entry *entry_{nullptr};
        CompressionManager *owner_{nullptr};
        std::shared_ptr<compression::DictCompression> compression_{};
    };

    CompressionManager(AsyncIoManager *io_mgr, const KvOptions *options);

    std::pair<Handle, KvError> GetOrLoad(const TableIdent &tbl_id,
                                         const DictMeta &meta);
    void UpdateDictionary(
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
    struct KeyHash
    {
        size_t operator()(const Key &key) const;
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
    std::unordered_map<Key, Entry, KeyHash> entries_;
    Entry lru_head_{};
    Entry lru_tail_{};
};
}  // namespace eloqstore
