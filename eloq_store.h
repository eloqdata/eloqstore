#pragma once

#include <atomic>

#include "error.h"
#include "scan_task.h"
#include "table_ident.h"
#include "write_task.h"

namespace kvstore
{
class Worker;

enum class RequestType : uint8_t
{
    Read,
    Scan,
    Write,
    Truncate
};

class KvRequest
{
public:
    virtual RequestType Type() const = 0;
    KvError Error() const;
    const char *ErrMessage() const;
    const TableIdent &TableId() const;
    uint64_t UserData() const;

    /**
     * @brief Test if this request is done.
     */
    bool IsDone() const;

protected:
    void Wait();
    void SetDone(KvError err);

    TableIdent tbl_id_;
    uint64_t user_data_{0};
    std::function<void(KvRequest *)> callback_{nullptr};
    std::atomic<bool> done_{false};
    KvError err_{KvError::NoError};

    friend class Worker;
    friend class EloqStore;
};

class ReadRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Read;
    }
    void SetArgs(TableIdent tid, std::string_view key);

    // input
    std::string_view key_;
    // output
    std::string value_;
    uint64_t ts_{0};
};

class ScanRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Scan;
    }
    void SetArgs(TableIdent tid, std::string_view begin, std::string_view end);

    // input
    std::string_view begin_key_;
    std::string_view end_key_;
    // output
    std::vector<KvEntry> entries_;
};

class WriteRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Write;
    }
    void SetArgs(TableIdent tid, std::vector<WriteDataEntry> &&batch);

    // input
    std::vector<WriteDataEntry> batch_;
};

class TruncateRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Truncate;
    }
    void SetArgs(TableIdent tid, std::string_view position);

    // input
    std::string_view position_;
};

class EloqStore
{
public:
    EloqStore(const KvOptions &opts);
    EloqStore(const EloqStore &) = delete;
    EloqStore(EloqStore &&) = delete;
    ~EloqStore();
    KvError Start();
    void Stop();

    template <typename F>
    bool ExecAsyn(KvRequest *req, uint64_t data, F callback)
    {
        req->user_data_ = data;
        req->callback_ = std::move(callback);
        return SendRequest(req);
    }

    bool ExecSync(KvRequest *req);

private:
    bool SendRequest(KvRequest *req);
    bool IsStopped() const;
    KvError InitDBDir();
    void CloseDBDir();

    int dir_fd_{-1};
    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<bool> stopped_;
    const KvOptions options_;
    friend Worker;
};
}  // namespace kvstore