#pragma once

#include <zstd.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace eloqstore::compression
{
enum class CompressionType : uint8_t
{
    None = 0,
    Dictionary,
    Standalone,
};

struct ZstdCCtxDeleter
{
    void operator()(ZSTD_CCtx *ptr) const noexcept;
};

struct ZstdDCtxDeleter
{
    void operator()(ZSTD_DCtx *ptr) const noexcept;
};

struct ZstdCDictDeleter
{
    void operator()(ZSTD_CDict *ptr) const noexcept;
};

struct ZstdDDictDeleter
{
    void operator()(ZSTD_DDict *ptr) const noexcept;
};

class DictCompression
{
public:
    static constexpr size_t kMaxDictBytes = 16 * 1024;
    static constexpr size_t kSampleTargetBytes = 100 * kMaxDictBytes;
    static constexpr size_t kMinSamples = 5;

    DictCompression() = default;
    DictCompression(const DictCompression &other) = delete;
    DictCompression &operator=(const DictCompression &other) = delete;
    DictCompression(DictCompression &&) noexcept = delete;
    DictCompression &operator=(DictCompression &&) noexcept = delete;
    ~DictCompression() = default;

    bool HasDictionary() const;
    const std::string &DictionaryBytes() const;
    void LoadDictionary(std::string &&dict_bytes);

    bool Dirty() const;
    void ClearDirty();

    void ClearSamples();
    void AddSample(const std::string &sample);
    void SampleAndBuildDictionaryIfNeeded(
        const std::span<WriteDataEntry> &entries);
    size_t SampleCount();
    void BuildDictionary();

    size_t DictionaryMemoryBytes() const;

    bool Compress(std::string_view input, std::string &output) const;
    bool Decompress(std::string_view input, std::string &output) const;

private:
    bool EnsureZstdObjects(int compression_level);

    bool dirty_{false};
    bool has_dictionary_{false};
    std::string dictionary_;
    std::string sample_data_;
    std::vector<size_t> sample_sizes_;
    std::unique_ptr<ZSTD_CCtx, ZstdCCtxDeleter> cctx_;
    std::unique_ptr<ZSTD_DCtx, ZstdDCtxDeleter> dctx_;
    std::unique_ptr<ZSTD_CDict, ZstdCDictDeleter> cdict_;
    std::unique_ptr<ZSTD_DDict, ZstdDDictDeleter> ddict_;
};

struct PreparedValue
{
    std::string_view data;
    CompressionType compression_kind{CompressionType::None};
};

bool CompressRaw(std::string_view input,
                 std::string &output,
                 int compression_level);
bool DecompressRaw(std::string_view input, std::string &output);

PreparedValue Prepare(std::string_view value,
                      DictCompression *compression,
                      std::string &scratch);

}  // namespace eloqstore::compression
