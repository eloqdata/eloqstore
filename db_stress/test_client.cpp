#include <gflags/gflags.h>

#include <iostream>

#include "../test_utils.h"
#include "../utils.h"

DEFINE_string(options, "", "path to ini config file");
DEFINE_string(partition, "", "table partition id");
DEFINE_string(operation, "", "scan/write");
DEFINE_string(begin_key, "", "scan begin key");
DEFINE_string(end_key, "", "scan end key");
DEFINE_uint32(value_len, 100, "value length");

void BatchWrite(eloqstore::EloqStore &store,
                const eloqstore::TableIdent &tbl_id,
                uint64_t begin_id,
                uint64_t end_id)
{
    const uint64_t now_ts = utils::UnixTs<chrono::milliseconds>();
    const eloqstore::WriteOp upsert = eloqstore::WriteOp::Upsert;
    std::vector<eloqstore::WriteDataEntry> entries;
    for (size_t idx = begin_id; idx < end_id; ++idx)
    {
        std::string key = test_util::Key(idx, 7);
        std::string val = test_util::Value(now_ts, FLAGS_value_len);
        entries.emplace_back(key, val, now_ts, upsert, 0);
    }
    eloqstore::BatchWriteRequest req;
    req.SetArgs(tbl_id, std::move(entries));
    store.ExecSync(&req);
    if (req.Error() != eloqstore::KvError::NoError)
    {
        LOG(FATAL) << "Write failed: " << req.ErrMessage() << std::endl;
    }
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    eloqstore::KvOptions options;
    if (!FLAGS_options.empty())
    {
        if (int res = options.LoadFromIni(FLAGS_options.c_str()); res != 0)
        {
            LOG(FATAL) << "Failed to parse " << FLAGS_options << " at " << res;
        }
    }

    auto tbl_id = eloqstore::TableIdent::FromString(FLAGS_partition);
    if (!tbl_id.IsValid())
    {
        LOG(FATAL) << "Invalid argument: " << FLAGS_partition << std::endl;
    }

    eloqstore::EloqStore store(options);
    eloqstore::KvError err = store.Start();
    if (err != eloqstore::KvError::NoError)
    {
        LOG(FATAL) << eloqstore::ErrorString(err) << std::endl;
    }

    if (FLAGS_operation == "scan")
    {
        eloqstore::ScanRequest req;
        req.SetArgs(tbl_id, FLAGS_begin_key, FLAGS_end_key);
        store.ExecSync(&req);
        if (req.Error() != eloqstore::KvError::NoError)
        {
            LOG(FATAL) << "Scan failed: " << req.ErrMessage() << std::endl;
        }
        std::cout << test_util::FormatEntries(req.Entries()) << std::endl;
    }
    else if (FLAGS_operation == "write")
    {
        uint64_t begin_id = std::stoi(FLAGS_begin_key);
        uint64_t end_id = std::stoi(FLAGS_end_key);
        for (size_t i = 0; i < 10; i++)
        {
            BatchWrite(store, tbl_id, begin_id, end_id);
        }
    }

    store.Stop();
}