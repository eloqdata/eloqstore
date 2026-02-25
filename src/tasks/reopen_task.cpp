#include "tasks/reopen_task.h"

#include <algorithm>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "async_io_manager.h"
#include "storage/index_page_manager.h"
#include "storage/shard.h"
#include "tasks/prewarm_task.h"

namespace eloqstore
{

KvError ReopenTask::Reopen(const TableIdent &tbl_id)
{
    KvError err =
        shard->IndexManager()->InstallExternalSnapshot(tbl_id, cow_meta_);
    if (err == KvError::NoError && !Options()->cloud_store_path.empty())
    {
        auto *cloud_mgr = static_cast<CloudStoreMgr *>(shard->IoManager());
        if (Options()->prewarm_cloud_cache && cow_meta_.mapper_ != nullptr)
        {
            MappingSnapshot *mapping = cow_meta_.mapper_->GetMapping();
            if (mapping != nullptr)
            {
                absl::flat_hash_set<FileId> referenced_files;
                const auto &mapping_tbl = mapping->mapping_tbl_;
                const uint8_t pages_shift = Options()->pages_per_file_shift;
                for (PageId page_id = 0; page_id < mapping_tbl.size();
                     ++page_id)
                {
                    FilePageId file_page_id =
                        mapping->ToFilePage(mapping_tbl.Get(page_id));
                    if (file_page_id == MaxFilePageId)
                    {
                        continue;
                    }
                    referenced_files.insert(file_page_id >> pages_shift);
                }

                if (!referenced_files.empty())
                {
                    std::vector<FileId> sorted_file_ids(referenced_files.begin(),
                                                        referenced_files.end());
                    std::sort(sorted_file_ids.begin(),
                              sorted_file_ids.end(),
                              std::greater<FileId>());

                    std::vector<PrewarmFile> prewarm_files;
                    prewarm_files.reserve(sorted_file_ids.size());
                    for (FileId file_id : sorted_file_ids)
                    {
                        PrewarmFile file;
                        file.tbl_id = tbl_id;
                        file.file_id = file_id;
                        file.term = cloud_mgr->GetFileIdTerm(tbl_id, file_id)
                                        .value_or(cloud_mgr->ProcessTerm());
                        file.file_size = 0;
                        file.is_manifest = false;
                        prewarm_files.emplace_back(std::move(file));
                    }
                    cloud_mgr->EnqueuePrewarmFiles(std::move(prewarm_files));
                }
            }
        }

        if (!shard->HasPendingLocalGc(tbl_id))
        {
            shard->AddPendingLocalGc(tbl_id);
        }
    }
    return err;
}

}  // namespace eloqstore
