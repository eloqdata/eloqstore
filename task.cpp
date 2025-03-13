#include "task.h"

#include <cassert>

#include "index_page_manager.h"
#include "task_manager.h"

namespace kvstore
{
void KvTask::Yield()
{
    main_ = main_.resume();
}

void KvTask::Resume()
{
    if (status_ != TaskStatus::Ongoing)
    {
        status_ = TaskStatus::Ongoing;
        task_mgr->scheduled_.Enqueue(this);
    }
}

int KvTask::WaitSyncIo()
{
    assert(inflight_io_ > 0);
    status_ = TaskStatus::WaitSyncIo;
    io_res_ = 0;
    io_flags_ = 0;
    Yield();
    return io_res_;
}

int KvTask::WaitAsynIo()
{
    asyn_io_err_ = 0;
    while (inflight_io_ > 0)
    {
        status_ = TaskStatus::WaitAllAsynIo;
        Yield();
    }
    return asyn_io_err_;
}

void KvTask::FinishIo(bool is_sync_io)
{
    assert(inflight_io_ > 0);
    inflight_io_--;
    switch (status_)
    {
    case TaskStatus::WaitSyncIo:
        if (is_sync_io)
        {
            Resume();
        }
        break;
    case TaskStatus::WaitAllAsynIo:
        if (inflight_io_ == 0)
        {
            Resume();
        }
        break;
    default:
        break;
    }
}

std::pair<DataPage, KvError> KvTask::LoadDataPage(const TableIdent &tbl_id,
                                                  uint32_t page_id,
                                                  uint32_t file_page_id)
{
    DataPage page(page_id, Options()->data_page_size);
    auto [ptr, err] = IoMgr()->ReadPage(tbl_id, file_page_id, page.GetPtr());
    page.SetPtr(std::move(ptr));
    if (err != KvError::NoError)
    {
        return {DataPage(), err};
    }
    return {std::move(page), KvError::NoError};
}

AsyncIoManager *IoMgr()
{
    return index_mgr->IoMgr();
}

const KvOptions *Options()
{
    return index_mgr->Options();
}

const Comparator *Comp()
{
    return Options()->comparator_;
}
}  // namespace kvstore