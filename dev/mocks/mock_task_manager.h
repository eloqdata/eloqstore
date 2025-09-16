#pragma once

#include <queue>
#include <memory>
#include "types.h"

namespace eloqstore::test {

// Forward declarations
class KvTask;
class TaskManager;

/**
 * Mock TaskManager for testing - minimal stub implementation
 */
class MockTaskManager {
public:
    MockTaskManager() = default;

    // Basic task tracking
    void SubmitTask(KvTask* task) {
        task_count_++;
        total_tasks_++;
    }

    void CompleteTask(KvTask* task) {
        if (task_count_ > 0) {
            task_count_--;
        }
        completed_tasks_++;
    }

    // Test control
    void SetFailureRate(double rate) { failure_rate_ = rate; }
    bool ShouldFail() const {
        if (failure_rate_ <= 0) return false;
        return (rand() / static_cast<double>(RAND_MAX)) < failure_rate_;
    }

    // Test inspection methods
    uint32_t GetActiveTaskCount() const { return task_count_; }
    uint64_t GetTotalTasks() const { return total_tasks_; }
    uint64_t GetCompletedTasks() const { return completed_tasks_; }

    void ResetCounters() {
        task_count_ = 0;
        total_tasks_ = 0;
        completed_tasks_ = 0;
    }

private:
    uint32_t task_count_ = 0;
    uint64_t total_tasks_ = 0;
    uint64_t completed_tasks_ = 0;
    double failure_rate_ = 0.0;
};

} // namespace eloqstore::test