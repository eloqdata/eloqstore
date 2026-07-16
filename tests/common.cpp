#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>
#include <string>

#include "utils.h"

eloqstore::EloqStore *InitStore(const eloqstore::KvOptions &opts, bool cleanup)
{
    static std::unique_ptr<eloqstore::EloqStore> eloq_store = nullptr;

    // Tear down any prior store before constructing the new one, so the old
    // destructor's worker-thread joins and LRU-cached fd releases finish
    // before we count the new store's fd budget below.
    //
    // Tests using this shared fixture must go through InitStore — never mix a
    // directly-owned store with this process-global instance. The global
    // Options()/Comp() plumbing assumes compatible live stores; overlapping
    // incompatible instances can leave teardown reading a nulled/foreign
    // global (observed as a flaky SIGSEGV in Prewarmer::Shutdown). Intentional
    // multi-instance/topology tests own and coordinate all instances instead.
    // Tests that only need to preserve on-disk/cloud state across generations
    // (warm restart, cache-trim) pass cleanup = false.
    if (eloq_store)
    {
        if (!eloq_store->IsStopped())
        {
            eloq_store->Stop();
        }
        eloq_store.reset();
    }
    if (cleanup)
    {
        if (!opts.cloud_store_path.empty())
        {
            S3TestClient s3_client(opts);
        }
        CleanupStore(opts);
    }

    // EloqStore::Start() counts the *process-wide* `/proc/self/fd` and
    // subtracts it from `fd_limit`. When multiple test cases run in the
    // same binary, glog log fds, minio HTTP sockets, catch2 internals, and
    // any fd that hasn't fully drained from a prior store all accumulate
    // in /proc/self/fd. Tests that pick a tight `fd_limit` to exercise
    // LRU-eviction (e.g. persist's "simple/complex LRU for opened fd" with
    // fd_limit = 20 + num_reserved_fd) then see Start() collapse
    // `shard_fd_limit` to 0 and return KvError::OpenFileLimit. Re-base the
    // budget on the current process fd count so the caller-provided limit
    // is honored as a *per-store* number regardless of how many tests ran
    // before this one.
    eloqstore::KvOptions adjusted_opts = opts;
    adjusted_opts.fd_limit += utils::CountUsedFD();

    // Recreate to ensure latest options are applied
    eloq_store = std::make_unique<eloqstore::EloqStore>(adjusted_opts);
    eloqstore::KvError err = eloq_store->Start(eloqstore::MainBranchName, 0);
    CHECK(err == eloqstore::KvError::NoError);
    return eloq_store.get();
}

bool ValidateFileSizes(const eloqstore::KvOptions &opts)
{
    bool all_valid = true;

    size_t max_data_file_size = opts.DataFileSize();
    size_t max_manifest_size = opts.manifest_limit;

    LOG(INFO) << "Validating file sizes - Max data file: "
              << (max_data_file_size / 1024 / 1024)
              << " MB, Max manifest: " << (max_manifest_size / 1024 / 1024)
              << " MB";

    for (const std::string &store_path : opts.store_path)
    {
        if (!std::filesystem::exists(store_path))
        {
            LOG(WARNING) << "Store path does not exist: " << store_path;
            continue;
        }

        try
        {
            for (const auto &entry :
                 std::filesystem::recursive_directory_iterator(store_path))
            {
                if (!entry.is_regular_file())
                {
                    continue;
                }

                std::string filename = entry.path().filename().string();
                size_t file_size = entry.file_size();

                if (filename.find("data_") == 0)
                {
                    if (file_size > max_data_file_size)
                    {
                        LOG(ERROR)
                            << "Data file exceeds size limit: " << entry.path()
                            << " (size: " << (file_size / 1024 / 1024)
                            << " MB, limit: "
                            << (max_data_file_size / 1024 / 1024) << " MB)";
                        all_valid = false;
                    }
                    else
                    {
                        LOG(INFO) << "✓ Data file size OK: " << filename << " ("
                                  << (file_size / 1024 / 1024) << " MB)";
                    }
                }
                else if (eloqstore::IsArchiveFile(filename))
                {
                    if (file_size > max_manifest_size)
                    {
                        LOG(ERROR)
                            << "Manifest file exceeds size limit: "
                            << entry.path() << " (size: " << (file_size / 1024)
                            << " KB, limit: " << (max_manifest_size / 1024)
                            << " KB)";
                        all_valid = false;
                    }
                    else
                    {
                        LOG(INFO) << "✓ Manifest file size OK: " << filename
                                  << " (" << (file_size / 1024) << " KB)";
                    }
                }
                else
                {
                    LOG(INFO) << "Other file: " << filename << " (" << file_size
                              << " bytes)";
                }
            }
        }
        catch (const std::exception &e)
        {
            LOG(ERROR) << "Error accessing store path " << store_path << ": "
                       << e.what();
            all_valid = false;
        }
    }

    if (all_valid)
    {
        LOG(INFO) << "✓ All file sizes are within limits";
    }
    else
    {
        LOG(ERROR) << "✗ Some files exceed size limits";
    }

    return all_valid;
}
