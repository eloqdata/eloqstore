#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>
#include <string>

eloqstore::EloqStore *InitStore(const eloqstore::KvOptions &opts)
{
    static std::unique_ptr<eloqstore::EloqStore> eloq_store = nullptr;

    if (eloq_store && !eloq_store->IsStopped())
    {
        eloq_store->Stop();
    }
    CleanupStore(opts);
    // Recreate to ensure latest options are applied
    eloq_store = std::make_unique<eloqstore::EloqStore>(opts);
    eloqstore::KvError err = eloq_store->Start();
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
