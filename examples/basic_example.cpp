#include <condition_variable>

#include "eloq_store.h"

std::mutex m;
std::condition_variable cv;
bool ready = false;

void wake_up(eloqstore::KvRequest *req)
{
    std::unique_lock lk(m);
    ready = true;
    lk.unlock();
    cv.notify_one();
}

int main()
{
    eloqstore::KvOptions opts;
    opts.store_path = {"/tmp/eloq_store"};
    opts.num_threads = 1;
    eloqstore::TableIdent tbl_id("t1", 1);

    eloqstore::EloqStore store(opts);
    eloqstore::KvError err = store.Start();
    assert(err == eloqstore::KvError::NoError);

    {
        eloqstore::BatchWriteRequest req;
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.emplace_back("key1", "val1", 1, eloqstore::WriteOp::Upsert);
        entries.emplace_back("key2", "val2", 1, eloqstore::WriteOp::Upsert);
        entries.emplace_back("key3", "val3", 1, eloqstore::WriteOp::Upsert);
        req.SetArgs(tbl_id, std::move(entries));
        bool ok = store.ExecAsyn(&req, 0, wake_up);
        assert(ok);
        {
            std::unique_lock lk(m);
            cv.wait(lk, [] { return ready; });
        }
        assert(req.Error() == eloqstore::KvError::NoError);
    }

    {
        ready = false;
        eloqstore::ReadRequest req;
        req.SetArgs(tbl_id, "key2");
        store.ExecAsyn(&req, 0, wake_up);
        {
            std::unique_lock lk(m);
            cv.wait(lk, [] { return ready; });
        }
        assert(req.Error() == eloqstore::KvError::NoError);
        assert(req.value_ == "val2");
        assert(req.ts_ == 1);
    }

    {
        // Execute asynchronously
        ready = false;
        eloqstore::ScanRequest req;
        req.SetArgs(tbl_id, "key1", "key3");
        store.ExecAsyn(&req, 0, wake_up);
        {
            std::unique_lock lk(m);
            cv.wait(lk, [] { return ready; });
        }
        assert(req.Entries().size() == 2);
        eloqstore::KvEntry &ent0 = req.Entries()[0];
        assert(ent0.key_ == "key1");
        assert(ent0.value_ == "val1");
        assert(ent0.timestamp_ == 1);
        eloqstore::KvEntry &ent1 = req.Entries()[1];
        assert(ent1.key_ == "key2");
        assert(ent1.value_ == "val2");
        assert(ent1.timestamp_ == 1);
    }

    {
        // Execute synchronously
        eloqstore::BatchWriteRequest req;
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.emplace_back("key1", "", 2, eloqstore::WriteOp::Delete);
        entries.emplace_back("key3", "val33", 2, eloqstore::WriteOp::Upsert);
        req.SetArgs(tbl_id, std::move(entries));
        store.ExecSync(&req);
        assert(req.Error() == eloqstore::KvError::NoError);
    }

    {
        eloqstore::ScanRequest req;
        req.SetArgs(tbl_id, "key3", "");
        store.ExecAsyn(&req, 0, wake_up);
        while (!req.IsDone())
            ;
        assert(req.Entries().size() == 1);
        eloqstore::KvEntry &ent0 = req.Entries()[0];
        assert(ent0.key_ == "key3");
        assert(ent0.value_ == "val33");
        assert(ent0.timestamp_ == 2);
    }

    store.Stop();
}