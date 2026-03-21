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
 */

#ifndef DSTORE_COMMON_ERROR_CODE_MAP_H
#define DSTORE_COMMON_ERROR_CODE_MAP_H

#include "errorcode/dstore_common_error_code.h"

namespace DSTORE {

static ErrorDetails g_common_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(COMMON_ERROR_UNDEFINED_ERROR)] =
        {"COMMON_ERROR_UNDEFINED_ERROR", "Undefined error"},
    [ERROR_GET_CODE(COMMON_ERROR_NODE_UNREACHABLE)] =
        {"COMMON_ERROR_NODE_UNREACHABLE", "Node (id=%u) cannot be reached"},
    [ERROR_GET_CODE(COMMON_ERROR_MEMORY_ALLOCATION)] =
        {"COMMON_ERROR_MEMORY_ALLOCATION", "Failed to allocate memory"},
    [ERROR_GET_CODE(COMMON_INFO_CONTACT_ENGINEER)] =
        {"COMMON_INFO_CONTACT_ENGINEER", "Please contact engineer to support."},
    [ERROR_GET_CODE(COMMON_ERROR_FUNCTION_RETURN_NULL)] =
        {"COMMON_ERROR_FUNCTION_RETURN_NULL", "function %u returned NULL"},
    [ERROR_GET_CODE(COMMON_ERROR_CREATE_THREAD_FAIL)] =
        {"COMMON_ERROR_CREATE_THREAD_FAIL", "failed to create thread"},
    [ERROR_GET_CODE(COMMON_ERROR_INIT_THREAD_FAIL)] =
        {"COMMON_ERROR_INIT_THREAD_FAIL", "failed to init thread"},
    [ERROR_GET_CODE(COMMON_ERROR_NO_ACTIVE_NODE)] =
        {"COMMON_ERROR_NO_ACTIVE_NODE", "No active node exists."},
    [ERROR_GET_CODE(TUPLESORT_ERROR_UNEXPECTED_END_OF_DATA)] =
        {"TUPLESORT_ERROR_UNEXPECTED_END_OF_DATA", "unexpected end of data"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_RANDOM_ACCESS_DISALLOWED)] =
        {"TUPLESORT_ERROR_RANDOM_ACCESS_DISALLOWED", "random access disallowed under parallel sort"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_INSUFFICIENT_MEMORY_ALLOWED)] =
        {"TUPLESORT_ERROR_INSUFFICIENT_MEMORY_ALLOWED", "insufficient memory allowed for sort"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_TYPE)] =
        {"TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_TYPE", "missing support function %d(%u,%u)"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_FUNCOID)] =
        {"TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_FUNCOID", "missing support function with funcOid=%u"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_UNEXPECTED_SORT_SUPPORT_STRATEGY)] =
        {"TUPLESORT_ERROR_UNEXPECTED_SORT_SUPPORT_STRATEGY", "unexpected sort support strategy: %d"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_UNEXPECTED_OUT_OF_MEMORY)] =
        {"TUPLESORT_ERROR_UNEXPECTED_OUT_OF_MEMORY", "unexpected out-of-memory situation in tuplesort"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_INVALID_TUPLESORT_STATE)] =
        {"TUPLESORT_ERROR_INVALID_TUPLESORT_STATE", "invalid tuplesort state"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_TOO_MANY_TUPLE)] =
        {"TUPLESORT_ERROR_TOO_MANY_TUPLE", "retrieved too many tuples in a bounded sort"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_UNEXPECTED_TAPE_POSITION)] =
        {"TUPLESORT_ERROR_UNEXPECTED_TAPE_POSITION", "unexpected tape position"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_BOGUS_TUPLE_LENGTH)] =
        {"TUPLESORT_ERROR_BOGUS_TUPLE_LENGTH", "bogus tuple length in backward scan"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_UNEXPECTED_END_OF_TAPE)] =
        {"TUPLESORT_ERROR_UNEXPECTED_END_OF_TAPE", "unexpected end of tape"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_COULD_NOT_CREATE_UNIQUE_INDEX)] =
        {"TUPLESORT_ERROR_COULD_NOT_CREATE_UNIQUE_INDEX", "could not create unique index."},
    [ERROR_GET_CODE(TUPLESORT_ERROR_SHOULD_NOT_BE_CALLED)] =
        {"TUPLESORT_ERROR_SHOULD_NOT_BE_CALLED", "copytup_index() should not be called"},
    [ERROR_GET_CODE(TUPLESORT_ERROR_TOO_MANY_RUNS_FOR_EXTERNAL_SORT)] =
        {"TUPLESORT_ERROR_TOO_MANY_RUNS_FOR_EXTERNAL_SORT", "cannot have more than %d runs for an external sort"},
    [ERROR_GET_CODE(TUPLESORT_INFO_BEGIN_INDEX_SORT)] =
        {"TUPLESORT_INFO_BEGIN_INDEX_SORT", "begin index sort: unique = %c, workMem = %d, randomAccess = %c"},
    [ERROR_GET_CODE(TUPLESORT_INFO_SORT_DONE)] =
        {"TUPLESORT_INFO_SORT_DONE", "%s of worker %d ended, %ld disk blocks used: %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_SWITCH_TO_BOUNDED_HEAPSORT)] =
        {"TUPLESORT_INFO_SWITCH_TO_BOUNDED_HEAPSORT", "switching to bounded heapsort at %d tuples: %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_PERFORM_SORT_START)] =
        {"TUPLESORT_INFO_PERFORM_SORT_START", "performsort of worker %d starting: %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_FINAL_MERGE)] =
        {"TUPLESORT_INFO_FINAL_MERGE", "performsort of worker %d done (except %d-way final merge): %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_PERFORM_SORT_DONE)] =
        {"TUPLESORT_INFO_PERFORM_SORT_DONE", "performsort of worker %d done: %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_SWITCH_TO_EXTERNAL_SORT)] =
        {"TUPLESORT_INFO_SWITCH_TO_EXTERNAL_SORT", "worker %d switching to external sort with %d tapes: %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_WORKER_USE_MEMORY)] =
        {"TUPLESORT_INFO_WORKER_USE_MEMORY", "worker %d using %zu KB of memory for tape buffers"},
    [ERROR_GET_CODE(TUPLESORT_INFO_START_MERGE_PASS)] =
        {"TUPLESORT_INFO_START_MERGE_PASS",
         "starting merge pass of %d input runs on %d tapes, %ld KB of memory for each input tape: %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_START_QUICKSORT)] =
        {"TUPLESORT_INFO_START_QUICKSORT", "worker %d starting quicksort of run %d: %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_FINISH_QUICKSORT)] =
        {"TUPLESORT_INFO_FINISH_QUICKSORT", "worker %d finished quicksort of run %d: %s"},
    [ERROR_GET_CODE(TUPLESORT_INFO_FINISH_WRITE_RUN)] =
        {"TUPLESORT_INFO_FINISH_WRITE_RUN", "worker %d finished writing run %d to tape %d: %s"},
    [ERROR_GET_CODE(STRING_ERROR_NO_DATA_LEFT_IN_MESSAGE)] =
        {"STRING_ERROR_NO_DATA_LEFT_IN_MESSAGE", "no data left in message"},
    [ERROR_GET_CODE(STRING_ERROR_INSUFFICIENT_DATA_LEFT)] =
        {"STRING_ERROR_INSUFFICIENT_DATA_LEFT", "insufficient data left in message"},
    [ERROR_GET_CODE(STRING_WARNING_SPIN_NOT_SUPPORT)] =
        {"STRING_WARNING_SPIN_NOT_SUPPORT", "spin.c does not support S_LOCK_FREE()"},
    [ERROR_GET_CODE(DATATYPE_ERROR_INVALID_DATUM_POINTER)] =
        {"DATATYPE_ERROR_INVALID_DATUM_POINTER", "invalid Datum pointer"},
    [ERROR_GET_CODE(DATATYPE_ERROR_INVALID_TYPLEN)] =
        {"DATATYPE_ERROR_INVALID_TYPLEN", "invalid typLen: %d"},
    [ERROR_GET_CODE(DATATYPE_ERROR_INVALID_CHARACTER_VALUE_FOR_CAST)] =
        {"DATATYPE_ERROR_INVALID_CHARACTER_VALUE_FOR_CAST", "could not convert string to UTF-16: error code %lu"},
    [ERROR_GET_CODE(DATATYPE_ERROR_INDETERMINATE_COLLATION)] =
        {"DATATYPE_ERROR_INDETERMINATE_COLLATION",
         "could not determine which collation to use for string comparison,"
         " Use the COLLATE clause to set the collation explicitly."},
    [ERROR_GET_CODE(STRING_ERROR_INVALID_STRING_ENLARGE)] =
        {"STRING_ERROR_INVALID_STRING_ENLARGE", "invalid string enlargement request size: %d"},
    [ERROR_GET_CODE(STRING_ERROR_CANNOT_ENLARGE_BUFFER)] =
        {"STRING_ERROR_CANNOT_ENLARGE_BUFFER", "Cannot enlarge buffer containing %d bytes by %d more bytes."},
    [ERROR_GET_CODE(STRING_ERROR_CANNOT_COMPARE_UNICODE_STRINGS)] =
        {"STRING_ERROR_CANNOT_COMPARE_UNICODE_STRINGS", "could not compare Unicode strings: %m"},
    [ERROR_GET_CODE(ARRAY_ERROR_NULL_VALUE_NOT_ALLOWED)] =
        {"ARRAY_ERROR_NULL_VALUE_NOT_ALLOWED", "null array element not allowed in this context"},
    [ERROR_GET_CODE(ARRAY_ERROR_DATATYPE_MISMATCH)] =
        {"ARRAY_ERROR_DATATYPE_MISMATCH", "cannot compare arrays of different element types"},
    [ERROR_GET_CODE(ARRAY_ERROR_ARRAY_SIZE_EXCEED_LIMIT)] =
        {"ARRAY_ERROR_ARRAY_SIZE_EXCEED_LIMIT", "array size exceeds the maximum allowed (%d)"},
    [ERROR_GET_CODE(LIST_ERROR_DOUBLY_NOT_NULL_VIOLATION)] =
        {"LIST_ERROR_DOUBLY_NOT_NULL_VIOLATION", "doubly linked list head address is NULL"},
    [ERROR_GET_CODE(LIST_ERROR_SINGLY_NOT_NULL_VIOLATION)] =
        {"LIST_ERROR_SINGLY_NOT_NULL_VIOLATION", "singly linked list head address is NULL"},
    [ERROR_GET_CODE(LIST_ERROR_DOUBLY_DATA_CORRUPTED)] =
        {"LIST_ERROR_DOUBLY_DATA_CORRUPTED", "doubly linked list is corrupted."},
    [ERROR_GET_CODE(SYSVSEMA_FATAL_FAIL_CREATE_SEMA)] =
        {"SYSVSEMA_FATAL_FAIL_CREATE_SEMA", "could not create semaphores: %m"},
    [ERROR_GET_CODE(SYSVSEMA_FATAL_FAIL_SYSTEM_CALL_SEMGET)] =
        {"SYSVSEMA_FATAL_FAIL_SYSTEM_CALL_SEMGET", "Failed system call was semget(%lu, %d, 0%o)."},
    [ERROR_GET_CODE(SYSVSEMA_INFO_WHY_CREATE_SEMA_FAIL)] =
        {"SYSVSEMA_INFO_WHY_CREATE_SEMA_FAIL",
         "This error does *not* mean that you have run out of disk space.  It occurs when either the system limit "
         "for the maximum number of semaphore sets (SEMMNI), or the system wide maximum number of semaphores (SEMMNS),"
         " would be exceeded.  You need to raise the respective kernel parameter.  Alternatively, reduce PostgreSQL's "
         "consumption of semaphores by reducing its max_connections parameter. The PostgreSQL documentation contains "
         "more information about configuring your system for PostgreSQL."},
    [ERROR_GET_CODE(SYSVSEMA_FATAL_SEMCTL_FAIL)] =
        {"SYSVSEMA_FATAL_SEMCTL_FAIL", "semctl(%d, %d, SETVAL, %d) failed: %m"},
    [ERROR_GET_CODE(SYSVSEMA_INFO_RAISE_SEMVMX_VALUE)] =
        {"SYSVSEMA_INFO_RAISE_SEMVMX_VALUE",
         "You possibly need to raise your kernel's SEMVMX value to be at least %d. "
         " Look into the PostgreSQL documentation for details."},
    [ERROR_GET_CODE(SYSVSEMA_FATAL_TOO_MANY_SEMAPHORES_CREATED)] =
        {"SYSVSEMA_FATAL_TOO_MANY_SEMAPHORES_CREATED", "too many semaphores created"},
    [ERROR_GET_CODE(SYSVSEM_FATAL_SEMOP_FAIL)] =
        {"SYSVSEM_FATAL_SEMOP_FAIL", "semop(id=%d) failed: %m"},
    [ERROR_GET_CODE(SYSTEM_FATAL_FUTEX_FAIL)] =
        {"SYSTEM_FATAL_FUTEX_FAIL", "futex failed: %m"},
    [ERROR_GET_CODE(LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK)] =
        {"LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK", "could not seek to block %ld of temporary file %s"},
    [ERROR_GET_CODE(LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK_READONLY)] =
        {"LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK_READONLY",
         "could not read block %ld of temporary file %s: read only %zu of %zu bytes"},
    [ERROR_GET_CODE(LOGTAPE_ERROR_INVALID_LOGTAPE_STATE)] =
        {"LOGTAPE_ERROR_INVALID_LOGTAPE_STATE", "invalid logtape state: should be dirty"},
    [ERROR_GET_CODE(LOGTAPE_ERROR_UNEXPECTED_END_OF_TAPE)] =
        {"LOGTAPE_ERROR_UNEXPECTED_END_OF_TAPE", "unexpected end of tape"},
    [ERROR_GET_CODE(LOGTAPE_ERROR_BROKEN_TAPE)] =
        {"LOGTAPE_ERROR_BROKEN_TAPE", "broken tape, next of block %ld is %ld, expected %ld"},
    [ERROR_GET_CODE(LOGTAPE_ERROR_INVALID_TAPE_SEEK_POSITION)] =
        {"LOGTAPE_ERROR_INVALID_TAPE_SEEK_POSITION", "invalid tape seek position"},
    [ERROR_GET_CODE(HASH_ERROR_MEMORY_SIZE_OVERFLOW)] =
        {"HASH_ERROR_MEMORY_SIZE_OVERFLOW", "requested shared memory size overflows size_t"},
    [ERROR_GET_CODE(HASH_ERROR_OUT_OF_MEMORY)] =
        {"HASH_ERROR_OUT_OF_MEMORY", "out of memory"},
    [ERROR_GET_CODE(HASH_ERROR_OUT_OF_SHARED_MEMORY)] =
        {"HASH_ERROR_OUT_OF_SHARED_MEMORY", "out of shared memory"},
    [ERROR_GET_CODE(HASH_ERROR_FAIL_TO_INITIALIZE_TABLE)] =
        {"HASH_ERROR_FAIL_TO_INITIALIZE_TABLE", "failed to initialize hash table \"%s\""},
    [ERROR_GET_CODE(HASH_ERROR_DIVISION_BY_ZERO)] =
        {"HASH_ERROR_DIVISION_BY_ZERO", "Division by zero when calculate element numbers!"},
    [ERROR_GET_CODE(HASH_ERROR_CANNOT_INSERT_INTO_FROZEN)] =
        {"HASH_ERROR_CANNOT_INSERT_INTO_FROZEN", "cannot insert into frozen hashtable \"%s\""},
    [ERROR_GET_CODE(HASH_ERROR_UNRECOGNIZED_HASH_ACTION_CODE)] =
        {"HASH_ERROR_UNRECOGNIZED_HASH_ACTION_CODE", "unrecognized hash action code: %d"},
    [ERROR_GET_CODE(HASH_ERROR_CANNOT_FREEZE_SHARED_HASHTABLE)] =
        {"HASH_ERROR_CANNOT_FREEZE_SHARED_HASHTABLE", "cannot freeze shared hashtable \"%s\""},
    [ERROR_GET_CODE(HASH_ERROR_CANNOT_FREEZE_ACTIVE_HASHTABLE)] =
        {"HASH_ERROR_CANNOT_FREEZE_ACTIVE_HASHTABLE", "cannot freeze hashtable \"%s\" because it has active scans"},
    [ERROR_GET_CODE(HASH_ERROR_CANNOT_TOO_MANY_ACTIVE_HASH_SEQ_SEARCH_SCAN)] =
        {"HASH_ERROR_CANNOT_TOO_MANY_ACTIVE_HASH_SEQ_SEARCH_SCAN",
         "too many active hash_seq_search scans, cannot start one on \"%s\""},
    [ERROR_GET_CODE(HASH_ERROR_NO_HASH_SEQ_SEARCH_SCAN)] =
        {"HASH_ERROR_NO_HASH_SEQ_SEARCH_SCAN", "no hash_seq_search scan for hash table \"%s\""},
    [ERROR_GET_CODE(HASH_WARNING_LEAKED_HASH_SEQ_SEARCH_SCAN)] =
        {"HASH_WARNING_LEAKED_HASH_SEQ_SEARCH_SCAN", "leaked hash_seq_search scan for hash table %s"},
    [ERROR_GET_CODE(BINARYHEAP_ERROR_OUT_OF_BINARY_HEAP_SLOTS)] =
        {"BINARYHEAP_ERROR_OUT_OF_BINARY_HEAP_SLOTS", "out of binary heap slots"},
    [ERROR_GET_CODE(ATTRIBUTE_ERROR_UNSUPPORTED_BYVAL_LENGTH)] =
        {"ATTRIBUTE_ERROR_UNSUPPORTED_BYVAL_LENGTH", "unsupported byval length: %d"},
    [ERROR_GET_CODE(RPC_ERROR_INTERNAL)] =
        {"RPC_ERROR_INTERNAL", "RPC_ERROR_INTERNAL in %s, component = %ld, module = %ld, errCode = %ld"},
    [ERROR_GET_CODE(RPC_ERROR_OUT_OF_MEMORY)] =
        {"RPC_ERROR_OUT_OF_MEMORY", "The communicatiom module failed to allocate more memory"},
    [ERROR_GET_CODE(RPC_ERROR_BAD_CONNECTION)] =
        {"RPC_ERROR_BAD_CONNECTION", "Communication module: bad connection"},
    [ERROR_GET_CODE(RPC_ERROR_INVALID_NODE_ID)] =
        {"RPC_ERROR_INVALID_NODE_ID", "Communication module has no config for the node id"},
    [ERROR_GET_CODE(RPC_ERROR_REJECTED_BY_HIGHER_TERM)] =
        {"RPC_ERROR_REJECTED_BY_HIGHER_TERM", "The request was rejected due to higher member view term."},
    [ERROR_GET_CODE(SQL_WARNING_REQUEST_ARE_CANCELED)] =
        {"SQL_WARNING_REQUEST_ARE_CANCELED", "Statements of the current request are canceled by user."},
    [ERROR_GET_CODE(BUFFILE_ERROR_FAIL_CREATE_TEMP_FILE)] =
        {"BUFFILE_ERROR_FAIL_CREATE_TEMP_FILE", "create tmp file fail"},
    [ERROR_GET_CODE(BUFFILE_ERROR_FAIL_WRITE_TEMP_FILE)] =
        {"BUFFILE_ERROR_FAIL_WRITE_TEMP_FILE", "write tmp file \"%s\" failed."},
    [ERROR_GET_CODE(BUFFER_ERROR_FAIL_READ_PAGE)] =
        {"BUFFER_ERROR_FAIL_READ_PAGE", "read page fail"},
    [ERROR_GET_CODE(RPC_ERROR_CONNECTION_HEART_BEAT_TIMEOUT)] =
        {"RPC_ERROR_CONNECTION_HEART_BEAT_TIMEOUT", "Communication broken due to heart beat timeout"},
    [ERROR_GET_CODE(COMMON_ERROR_TUPLESIZE_LIMIT_EXCEEDED)] =
        {"COMMON_ERROR_TUPLESIZE_LIMIT_EXCEEDED", "Exceeds the maximum limit of 1GB for a tuple"},
    [ERROR_GET_CODE(INVALID_ERROR_FAIL_CHECK_INVALID_COMPLETION)] =
        {"INVALID_ERROR_FAIL_CHECK_INVALID_COMPLETION", "Check invalidation completion failed."}
};

static_assert(sizeof(g_common_error_code_map) != COMMON_ERROR_REASON_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
