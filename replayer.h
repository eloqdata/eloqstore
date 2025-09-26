#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "async_io_manager.h"
#include "error.h"
#include "page_mapper.h"

namespace eloqstore
{
class Replayer
{
public:
    Replayer(const KvOptions *opts);
    KvError Replay(ManifestFile *file);
    std::unique_ptr<PageMapper> GetMapper(IndexPageManager *idx_mgr,
                                          const TableIdent *tbl_ident);

    PageId root_;
    PageId ttl_root_;
    std::vector<uint64_t> mapping_tbl_;
    FilePageId max_fp_id_;
    uint64_t file_size_;

private:
    KvError ParseNextRecord(ManifestFile *file);
    void DeserializeSnapshot(std::string_view snapshot);
    void ReplayLog();

    const KvOptions *opts_;
    std::string log_buf_;
    std::string_view payload_;
};
}  // namespace eloqstore