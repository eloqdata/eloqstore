#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "data_page.h"
#include "kv_options.h"
#include "page.h"

namespace eloqstore
{

class DataPageBuilder
{
public:
    explicit DataPageBuilder(const KvOptions *opt);

    DataPageBuilder(const DataPageBuilder &) = delete;
    DataPageBuilder &operator=(const DataPageBuilder &) = delete;

    // Reset the contents as if the DataPageBuilder was just constructed.
    void Reset();

    // REQUIRES: Finish() has not been called since the last call to Reset().
    // REQUIRES: key is larger than any previously added key
    bool Add(std::string_view key,
             std::string_view value,
             bool overflow,
             uint64_t ts,
             uint64_t expire_ts,
             compression::CompressionType compression_kind);

    // Finish building the block and return a view that refers to the page
    // contents. The returned view will remain valid for the lifetime of this
    // builder or until Reset() is called.
    std::string_view Finish();

    // Returns an estimate of the current (uncompressed) size of the block
    // we are building.
    size_t CurrentSizeEstimate() const;
    static bool IsOverflowKV(std::string_view key,
                             size_t val_size,
                             uint64_t ts,
                             uint64_t expire_ts,
                             const KvOptions *options);

    // Return true iff no entries have been added since the last Reset()
    bool IsEmpty() const
    {
        return cnt_ == 0;
    }

    bool NeedRestart() const
    {
        return counter_ >= options_->data_page_restart_interval;
    }

    static size_t HeaderSize();

private:
    static std::tuple<size_t, size_t, size_t, uint64_t> CalculateDelta(
        std::string_view key,
        size_t val_size,
        uint64_t ts,
        uint64_t expire_ts,
        std::string_view last_key,
        uint64_t last_ts,
        bool restart);

    const KvOptions *const options_;
    std::string buffer_;              // Destination buffer
    std::vector<uint32_t> restarts_;  // Restart points
    int counter_;                     // Number of entries emitted since restart
    uint16_t cnt_{0};
    bool finished_;  // Has Finish() been called?
    std::string last_key_;
    int64_t last_timestamp_{0};
};

class FastPageBuilder
{
public:
    explicit FastPageBuilder(const KvOptions *options);
    void Reset(char *ptr, PageType type);
    bool AddRegion(std::string_view region);
    size_t CurrentSize() const;
    void Finish();

private:
    const KvOptions *const options_;
    char *ptr_{nullptr};
    uint16_t end_offset_{0};
    std::vector<uint16_t> region_offsets_;
};

}  // namespace eloqstore
