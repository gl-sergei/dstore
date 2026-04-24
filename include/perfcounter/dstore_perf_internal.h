/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * Description: The internal message of performance monitor.
 */

#ifndef DSTORE_PERF_INTERNAL_H
#define DSTORE_PERF_INTERNAL_H
#include "inttypes.h"
namespace DSTORE {
// todo:fix (std::nothrow, __FILE__, __LINE__)
#define NEW(mc) new
#define NEW_ARRAY(mc) NEW(mc)

#define DELETE(ptr) (delete (ptr))
#define DELETE_AND_RESET(ptr) \
    do {                    \
        DELETE((ptr));      \
        (ptr) = nullptr;    \
    } while (0)

#define DELETE_ARRAY(ptr) (delete[] (ptr))
constexpr uint32 MAX_UINT32{~0U};

#define DELETE_ARRAY_AND_RESET(ptr) \
    do {                         \
        DELETE_ARRAY((ptr));     \
        (ptr) = nullptr;         \
    } while (0)

#define FMT_U64 "%" PRIu64
#define FMT_D64 "%" PRId64
using PerfId = uint32;

constexpr int32 PERF_ITEM_LOG_MAX_SIZE = 128;
constexpr uint32 STAT_ITEM_MAX_SIZE = 256;
constexpr uint32 PERF_DUMPBUFFER_INIT_SIZE = 1024 * 1024;
constexpr uint32 PERF_DUMPBUFFER_EXTEND_SIZE = 4 * 1024 * 1024;
constexpr uint32 PERF_DUMPBUFFER_MAX_SIZE = 128 * 1024 * 1024;
constexpr int32 PERF_DUMPBUFER_OUT_OF_MEMORY = -1;

constexpr PerfId PERF_STAT_INVALID_ID = 0;
constexpr PerfId PERF_STAT_INITIAL_ID = 1;

/* PERF_UNIT_MAX_LEVEL + 1 */
constexpr uint8 PERF_ITEM_MAX_INDENT_LEVEL = 5;
/* Extra indent: used for PartitionStat */
constexpr char PERF_ITEM_INDENT[PERF_ITEM_MAX_INDENT_LEVEL + 1] = {'\t', '\t', '\t', '\t', '\t', '\t'};

constexpr uint32 RANGE_PARTITION_EXTRA_BUCKETS = 2;
constexpr uint32 STATIS_PERCENTILE_95TH = 95;

template <typename T>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT = nullptr;
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT<uint8> = "%s id=%u: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT<int8> = "%s id=%u: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT<uint16> = "%s id=%u: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT<int16> = "%s id=%u: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT<uint32> = "%s id=%u: avg/min/max/sum=%u/%u/%u/%u cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT<int32> = "%s id=%u: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT<uint64> =
    "%s id=%u: avg/min/max/sum=" FMT_U64 "/" FMT_U64 "/" FMT_U64 "/" FMT_U64 " cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT<int64> =
    "%s id=%u: avg/min/max/sum=" FMT_D64 "/" FMT_D64 "/" FMT_D64 "/" FMT_D64 " cnt=" FMT_U64 "\n";

template <typename T>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT = nullptr;
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT<uint8> =
    "%s id=%u, parent=%s: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT<int8> =
    "%s id=%u, parent=%s: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT<uint16> =
    "%s id=%u, parent=%s: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT<int16> =
    "%s id=%u, parent=%s: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT<uint32> =
    "%s id=%u, parent=%s: avg/min/max/sum=%u/%u/%u/%u cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT<int32> =
    "%s id=%u, parent=%s: avg/min/max/sum=%d/%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT<uint64> =
    "%s id=%u, parent=%s: avg/min/max/sum=" FMT_U64 "/" FMT_U64 "/" FMT_U64 "/" FMT_U64 " cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_NUMERIC_FORMAT_WITH_PARENT<int64> =
    "%s id=%u, parent=%s: avg/min/max/sum=" FMT_D64 "/" FMT_D64 "/" FMT_D64 "/" FMT_D64 " cnt=" FMT_U64 "\n";

template <typename T>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT = nullptr;
template <>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT<uint8> = "bucket %d: (MIN, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT<int8> = "bucket %d: (MIN, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT<uint16> = "bucket %d: (MIN, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT<int16> = "bucket %d: (MIN, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT<uint32> = "bucket %d: (MIN, %u) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT<int32> = "bucket %d: (MIN, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT<uint64> = "bucket %d: (MIN, " FMT_U64 ") cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_ORIGIN_FORMAT<int64> = "bucket %d: (MIN, " FMT_D64 ") cnt=%u\n";

template <typename T>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT = nullptr;
template <>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT<uint8> = "bucket %d: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT<int8> = "bucket %d: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT<uint16> = "bucket %d: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT<int16> = "bucket %d: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT<uint32> = "bucket %d: [%u, %u) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT<int32> = "bucket %d: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT<uint64> = "bucket %d: [" FMT_U64 ", " FMT_U64 ") cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_NORMAL_FORMAT<int64> = "bucket %d: [" FMT_D64 ", " FMT_D64 ") cnt=%u\n";

template <typename T>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT = nullptr;
template <>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT<uint8> = "bucket %d: [%d, MAX) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT<int8> = "bucket %d: [%d, MAX) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT<uint16> = "bucket %d: [%d, MAX) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT<int16> = "bucket %d: [%d, MAX) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT<uint32> = "bucket %d: [%u, MAX) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT<int32> = "bucket %d: [%d, MAX) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT<uint64> = "bucket %d: [" FMT_U64 ", MAX) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_END_FORMAT<int64> = "bucket %d: [" FMT_D64 ", MAX) cnt=%u\n";

template <typename T>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT = nullptr;
template <>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT<uint8> = "Percentile%u: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT<int8> = "Percentile%u: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT<uint16> = "Percentile%u: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT<int16> = "Percentile%u: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT<uint32> = "Percentile%u: [%u, %u) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT<int32> = "Percentile%u: [%d, %d) cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT<uint64> = "Percentile%u: [" FMT_U64 ", " FMT_U64 ") cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_RANGEPART_PERCENTILE_FORMAT<int64> = "Percentile%u: [" FMT_D64 ", " FMT_D64 ") cnt=%u\n";

template <typename T>
inline constexpr const char *PERFLOG_LISTPART_FORMAT = nullptr;
template <>
inline constexpr const char *PERFLOG_LISTPART_FORMAT<uint8> = "bucket '%d': cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_LISTPART_FORMAT<int8> = "bucket '%d': cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_LISTPART_FORMAT<uint16> = "bucket '%d': cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_LISTPART_FORMAT<int16> = "bucket '%d': cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_LISTPART_FORMAT<uint32> = "bucket '%u': cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_LISTPART_FORMAT<int32> = "bucket '%d': cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_LISTPART_FORMAT<uint64> = "bucket '" FMT_U64 "': cnt=%u\n";
template <>
inline constexpr const char *PERFLOG_LISTPART_FORMAT<int64> = "bucket '" FMT_D64 "': cnt=%u\n";

template <typename T>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT = nullptr;
template <>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT<uint8> = "avg/min/max=%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT<int8> = "avg/min/max=%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT<uint16> = "avg/min/max=%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT<int16> = "avg/min/max=%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT<uint32> = "avg/min/max=%u/%u/%u cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT<int32> = "avg/min/max=%d/%d/%d cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT<uint64> =
    "avg/min/max=" FMT_U64 "/" FMT_U64 "/" FMT_U64 " cnt=" FMT_U64 "\n";
template <>
inline constexpr const char *PERFLOG_TIMEPART_NUMERIC_FORMAT<int64> =
    "avg/min/max=" FMT_D64 "/" FMT_D64 "/" FMT_D64 "/" FMT_D64 " cnt=" FMT_U64 "\n";

}  // namespace DSTORE
#endif /* STORAGE_PERF_INTERNAL_H */
