#include "compression.h"

#include <glog/logging.h>
#include <zdict.h>
#include <zstd.h>

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "coding.h"

namespace eloqstore::compression
{
namespace
{
thread_local std::unique_ptr<ZSTD_CCtx, ZstdCCtxDeleter> kCompressCtx{
    ZSTD_createCCtx()};
thread_local std::unique_ptr<ZSTD_DCtx, ZstdDCtxDeleter> kDecompressCtx{
    ZSTD_createDCtx()};
constexpr size_t kSkipCompressionThreshold = 100;
constexpr size_t kStandaloneCompressionThreshold = 10ULL * 1024 * 1024;
constexpr int kCompressionLevel = ZSTD_CLEVEL_DEFAULT;
}  // namespace

bool CompressRaw(std::string_view input, std::string &output)
{
    if (!kCompressCtx)
    {
        kCompressCtx.reset(ZSTD_createCCtx());
        if (!kCompressCtx)
        {
            output.clear();
            return false;
        }
    }

    size_t compressed_data_offset = Varint32Size(input.size());
    size_t max_compressed_len = ZSTD_compressBound(input.size());
    output.resize(compressed_data_offset + max_compressed_len);
    EncodeVarint32(&output[0], static_cast<uint32_t>(input.size()));
    size_t written = ZSTD_compressCCtx(kCompressCtx.get(),
                                       &output[compressed_data_offset],
                                       max_compressed_len,
                                       &input[0],
                                       input.size(),
                                       kCompressionLevel);
    if (ZSTD_isError(written) || written == 0 || written >= input.size())
    {
        output.clear();
        return false;
    }
    output.resize(compressed_data_offset + written);
    return true;
}

bool DecompressRaw(std::string_view input, std::string &output)
{
    if (!kDecompressCtx)
    {
        kDecompressCtx.reset(ZSTD_createDCtx());
        if (!kDecompressCtx)
        {
            output.clear();
            return false;
        }
    }
    uint32_t original_len;
    if (!GetVarint32(&input, &original_len))
    {
        return false;
    }

    output.resize(original_len);
    size_t decoded = ZSTD_decompressDCtx(kDecompressCtx.get(),
                                         &output[0],
                                         original_len,
                                         &input[0],
                                         input.size());
    if (ZSTD_isError(decoded) || decoded != original_len)
    {
        output.clear();
        return false;
    }
    return true;
}

PreparedValue Prepare(std::string_view value,
                      DictCompression *compression,
                      std::string &scratch)
{
    scratch.clear();

    if (value.size() < kSkipCompressionThreshold)
    {
        return {value, CompressionType::None};
    }

    if (value.size() > kStandaloneCompressionThreshold)
    {
        if (CompressRaw(value, scratch))
        {
            return {scratch, CompressionType::Standalone};
        }
        return {value, CompressionType::None};
    }

    assert(compression != nullptr);
    if (compression->HasDictionary() && compression->Compress(value, scratch))
    {
        return {scratch, CompressionType::Dictionary};
    }

    return {value, compression::CompressionType::None};
}
void ZstdCCtxDeleter::operator()(ZSTD_CCtx *ptr) const noexcept
{
    if (ptr != nullptr)
    {
        ZSTD_freeCCtx(ptr);
    }
}

void ZstdDCtxDeleter::operator()(ZSTD_DCtx *ptr) const noexcept
{
    if (ptr != nullptr)
    {
        ZSTD_freeDCtx(ptr);
    }
}

void ZstdCDictDeleter::operator()(ZSTD_CDict *ptr) const noexcept
{
    if (ptr != nullptr)
    {
        ZSTD_freeCDict(ptr);
    }
}

void ZstdDDictDeleter::operator()(ZSTD_DDict *ptr) const noexcept
{
    if (ptr != nullptr)
    {
        ZSTD_freeDDict(ptr);
    }
}

bool DictCompression::HasDictionary() const
{
    return has_dictionary_;
}

const std::string &DictCompression::DictionaryBytes() const
{
    return dictionary_;
}

void DictCompression::LoadDictionary(std::string &&dict_bytes)
{
    dictionary_ = std::move(dict_bytes);
    has_dictionary_ = true;
    if (!EnsureZstdObjects())
    {
        LOG(FATAL) << "Manifest is corrupted";
    }
}

bool DictCompression::Dirty() const
{
    return dirty_;
}

void DictCompression::ClearDirty()
{
    dirty_ = false;
}

void DictCompression::ClearSamples()
{
    sample_data_.clear();
    sample_sizes_.clear();
}

void DictCompression::AddSample(const std::string &sample)
{
    sample_data_ += sample;
    sample_sizes_.push_back(sample.size());
};

void DictCompression::SampleAndBuildDictionaryIfNeeded(
    const std::span<WriteDataEntry> &entries)
{
    if (HasDictionary())
    {
        return;
    }
    size_t total_size = 0;
    std::vector<size_t> valid_indices;
    valid_indices.reserve(entries.size());
    for (size_t i = 0; i < entries.size(); ++i)
    {
        size_t len = entries[i].val_.size();
        if (len >= kSkipCompressionThreshold &&
            len <= kStandaloneCompressionThreshold)
        {
            valid_indices.push_back(i);
            total_size += len;
        }
    }
    if (valid_indices.empty())
    {
        return;
    }
    const size_t remaining = kSampleTargetBytes - sample_data_.size();
    if (total_size < remaining)
    {
        for (size_t idx : valid_indices)
        {
            AddSample(entries[idx].val_);
        }
        return;
    }
    thread_local std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, valid_indices.size() - 1);

    const bool need_min_samples =
        SampleCount() + valid_indices.size() >= kMinSamples;

    while (sample_data_.size() < kSampleTargetBytes ||
           (need_min_samples && SampleCount() < kMinSamples))
    {
        const auto &sample = entries[valid_indices[dist(rng)]].val_;
        AddSample(sample);
    }
    if (sample_data_.size() >= kSampleTargetBytes)
    {
        BuildDictionary();
    }
}

size_t DictCompression::SampleCount()
{
    return sample_sizes_.size();
}

void DictCompression::BuildDictionary()
{
    has_dictionary_ = true;
    dirty_ = true;

    std::string dict_buffer(kMaxDictBytes, '\0');
    const size_t trained = ZDICT_trainFromBuffer(dict_buffer.data(),
                                                 kMaxDictBytes,
                                                 sample_data_.data(),
                                                 sample_sizes_.data(),
                                                 sample_sizes_.size());
    ClearSamples();
    if (ZDICT_isError(trained) != 0)
    {
        dict_buffer = "";
    }
    dictionary_ = std::move(dict_buffer);
    if (!EnsureZstdObjects())
    {
        // In case that zstd objects cannot be created, use empty dictionary.
        dictionary_ = "";
        if (!EnsureZstdObjects())
        {
            LOG(FATAL) << "Fail to init zstd objects with empty dictionary";
        }
    }
}

bool DictCompression::Compress(std::string_view input,
                               std::string &output) const
{
    output.clear();
    if (!HasDictionary() || input.empty())
    {
        return false;
    }
    if (!cctx_ || !cdict_)
    {
        return false;
    }

    size_t compressed_data_offset = Varint32Size(input.size());
    size_t max_compressed_len = ZSTD_compressBound(input.size());
    output.resize(compressed_data_offset + max_compressed_len);
    EncodeVarint32(&output[0], static_cast<uint32_t>(input.size()));
    size_t written = ZSTD_compress_usingCDict(cctx_.get(),
                                              &output[compressed_data_offset],
                                              max_compressed_len,
                                              &input[0],
                                              input.size(),
                                              cdict_.get());
    if (ZSTD_isError(written) || written == 0 || written >= input.size())
    {
        return false;
    }
    output.resize(compressed_data_offset + written);
    return true;
}

bool DictCompression::Decompress(std::string_view input,
                                 std::string &output) const
{
    output.clear();
    assert(HasDictionary());
    uint32_t original_len;
    if (!GetVarint32(&input, &original_len))
    {
        return false;
    }

    output.resize(original_len);
    size_t decoded = ZSTD_decompress_usingDDict(dctx_.get(),
                                                &output[0],
                                                original_len,
                                                &input[0],
                                                input.size(),
                                                ddict_.get());
    if (ZSTD_isError(decoded) || decoded != original_len)
    {
        return false;
    }
    return true;
}

bool DictCompression::EnsureZstdObjects()
{
    cctx_.reset(ZSTD_createCCtx());
    cdict_.reset(ZSTD_createCDict(
        dictionary_.data(), dictionary_.size(), kCompressionLevel));
    dctx_.reset(ZSTD_createDCtx());
    ddict_.reset(ZSTD_createDDict(dictionary_.data(), dictionary_.size()));
    if (!cctx_ || !cdict_ || !dctx_ || !ddict_)
    {
        return false;
    }
    return true;
}

}  // namespace eloqstore::compression
