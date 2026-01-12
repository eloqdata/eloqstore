/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#ifdef ELOQSTORE_WITH_TXSERVICE
#include <cstddef>
#include <memory>
#include <string>

#include "meter.h"
#include "metrics.h"

namespace metrics
{
inline const Name NAME_ELOQSTORE_WORK_ONE_ROUND_DURATION{
    "eloqstore_work_one_round_duration"};
inline const Name NAME_ELOQSTORE_ASYNC_IO_SUBMIT_DURATION{
    "eloqstore_async_io_submit_duration"};
inline const Name NAME_ELOQSTORE_TASK_MANAGER_ACTIVE_TASKS{
    "eloqstore_task_manager_active_tasks"};
inline const Name NAME_ELOQSTORE_REQUEST_LATENCY{"eloqstore_request_latency"};
inline const Name NAME_ELOQSTORE_REQUESTS_COMPLETED{
    "eloqstore_requests_completed"};
}  // namespace metrics
#endif  // ELOQSTORE_WITH_TXSERVICE
