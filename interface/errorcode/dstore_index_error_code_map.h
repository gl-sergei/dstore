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

#ifndef DSTORE_INDEX_ERROR_CODE_MAP_H
#define DSTORE_INDEX_ERROR_CODE_MAP_H

#include "errorcode/dstore_index_error_code.h"

namespace DSTORE {

static ErrorDetails g_index_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_CREATE_BTREE_SMGR)] =
        {"INDEX_ERROR_FAIL_CREATE_BTREE_SMGR", "failed to create/init index segment. segment id is {%d, %u}"},
    [ERROR_GET_CODE(INDEX_ERROR_UNKNOWN_META_PAGE)] =
        {"INDEX_ERROR_UNKNOWN_META_PAGE", "btree meta page id is unknown. segment id is {%u, %u}"},
    [ERROR_GET_CODE(INDEX_ERROR_UNSUPPORTED_DATA_TYPE)] =
        {"INDEX_ERROR_UNSUPPORTED_DATA_TYPE", "unsupported data type %u"},
    [ERROR_GET_CODE(INDEX_ERROR_NO_LIVE_ROOT_FOUND)] =
        {"INDEX_ERROR_NO_LIVE_ROOT_FOUND", "no live root page found in index \"%s\""},
    [ERROR_GET_CODE(INDEX_ERROR_ROOT_PAGE_LEVEL_MISMATCH)] =
        {"INDEX_ERROR_ROOT_PAGE_LEVEL_MISMATCH", "root page %u,%u of index \"%s\" has level %u, expected %u"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_CREATE_NEW_PAGE)] =
        {"INDEX_ERROR_FAIL_CREATE_NEW_PAGE", "failed to create new page. segment id is {%u, %u}"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_BUILD_INDEX_FOR_PAGE_DAMAGED)] =
        {"INDEX_ERROR_FAIL_BUILD_INDEX_FOR_PAGE_DAMAGED", "page {%u, %u} is damaged while btree building"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_BUILD_INDEX_FOR_MISSING_ROOT)] =
        {"INDEX_ERROR_FAIL_BUILD_INDEX_FOR_MISSING_ROOT", "page {%u, %u} is neither root nor single in level"},
    [ERROR_GET_CODE(INDEX_ERROR_OVERWRITE_HIGHKEY)] =
        {"INDEX_ERROR_OVERWRITE_HIGHKEY", "failed to add high key to the index page."},
    [ERROR_GET_CODE(INDEX_ERROR_INSERT_UNIQUE_CHECK)] =
        {"INDEX_ERROR_INSERT_UNIQUE_CHECK", "duplicate key value violates unique constraint."},
    [ERROR_GET_CODE(INDEX_ERROR_MOVE_END)] =
        {"INDEX_ERROR_MOVE_END", "fell off the end of index \"%s\"."},
    [ERROR_GET_CODE(INDEX_ERROR_FAILED_TO_FIND_DELETING_TARGET)] =
        {"INDEX_ERROR_FAILED_TO_FIND_DELETING_TARGET", "failed to find the deleting target."},
    [ERROR_GET_CODE(INDEX_ERROR_NO_ROOT_PAGE)] =
        {"INDEX_ERROR_NO_ROOT_PAGE", "no live root page found in index \"%s\"."},
    [ERROR_GET_CODE(INDEX_ERROR_ROOT_LEVEL_NOT_MATCHED)] =
        {"INDEX_ERROR_ROOT_LEVEL_NOT_MATCHED",
         "root page %u,%u of index \"%s\" has level %u, expected %u. btree meta {%u, %u}"},
    [ERROR_GET_CODE(INDEX_ERROR_ADD_ITEM_FAIL)] =
        {"INDEX_ERROR_ADD_ITEM_FAIL", "failed to add item to the index page"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_SPLIT_FOR_NO_TD_SPACE)] =
        {"INDEX_ERROR_FAIL_SPLIT_FOR_NO_TD_SPACE", "split page %u failed for insufficient td space"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_REFIND_PARENT_KEY)] =
        {"INDEX_ERROR_FAIL_REFIND_PARENT_KEY", "failed to re-find parent key in index \"%s\" for split pages %u,%u"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_ADD_HIGH_KEY_TO_RIGHT)] =
        {"INDEX_ERROR_FAIL_ADD_HIGH_KEY_TO_RIGHT", "failed to add high key to right sibling when splitting"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_ADD_NEW_ITEM_TO_LEFT)] =
        {"INDEX_ERROR_FAIL_ADD_NEW_ITEM_TO_LEFT", "failed to add new item to left sibling when splitting"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_ADD_NEW_ITEM_TO_RIGHT)] =
        {"INDEX_ERROR_FAIL_ADD_NEW_ITEM_TO_RIGHT", "failed to add new item to right sibling when splitting"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_ADD_ORIGIN_ITEM_TO_LEFT)] =
        {"INDEX_ERROR_FAIL_ADD_ORIGIN_ITEM_TO_LEFT", "failed to add origin item to left sibling when splitting"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_ADD_ORIGIN_ITEM_TO_RIGHT)] =
        {"INDEX_ERROR_FAIL_ADD_ORIGIN_ITEM_TO_RIGHT", "failed to add origin item to right sibling when splitting"},
    [ERROR_GET_CODE(INDEX_ERROR_RIGHT_LEFT_LINK_NOT_MATCH)] =
        {"INDEX_ERROR_RIGHT_LEFT_LINK_NOT_MATCH", "right sibling's left-link doesn't match"},
    [ERROR_GET_CODE(INDEX_ERROR_UNRECOGNIZED_ROWCOMPARETYPE)] =
        {"INDEX_ERROR_UNRECOGNIZED_ROWCOMPARETYPE", "unrecognized RowCompareType: %d"},
    [ERROR_GET_CODE(INDEX_ERROR_COULD_NOT_FIND_LEFT_SIBLING)] =
        {"INDEX_ERROR_COULD_NOT_FIND_LEFT_SIBLING", "could not find left sibling of block %u in index \"%s\""},
    [ERROR_GET_CODE(INDEX_ERROR_INVALID_SCAN_DIRECTION)] =
        {"INDEX_ERROR_INVALID_SCAN_DIRECTION", "invalid scan direction: %d"},
    [ERROR_GET_CODE(INDEX_ERROR_COULD_NOT_FIND_FEASIBLE_SPLIT_POINT)] =
        {"INDEX_ERROR_COULD_NOT_FIND_FEASIBLE_SPLIT_POINT", "could not find a feasible split point for index \"%s\""},
    [ERROR_GET_CODE(INDEX_ERROR_UNRECOGNIZED_STRATEGY_NUMBER)] =
        {"INDEX_ERROR_UNRECOGNIZED_STRATEGY_NUMBER", "unrecognized StrategyNumber: %d"},
    [ERROR_GET_CODE(INDEX_ERROR_INDEX_KEYS_MUST_BE_ORDERED_BY_ATTRIBUTE)] =
        {"INDEX_ERROR_INDEX_KEYS_MUST_BE_ORDERED_BY_ATTRIBUTE", "btree index keys must be ordered by attribute"},
    [ERROR_GET_CODE(INDEX_ERROR_CHANGED_NO_PARENT_LEAF)] =
        {"INDEX_ERROR_CHANGED_NO_PARENT_LEAF", "leaf page {%u, %u} has been changed after unlinked from parent"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_BUILD_GLOBAL_INDEX)] =
        {"INDEX_ERROR_FAIL_BUILD_GLOBAL_INDEX",
         "failed to build global index for incorrect number of partition table manager"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_FOR_HUGE_INDEX_TUPLE)] =
        {"INDEX_ERROR_FAIL_FOR_HUGE_INDEX_TUPLE", "tuple size %u is greater than max available space of btree %u"},
    [ERROR_GET_CODE(INDEX_ERROR_FOR_INVALID_LAST_HIKEY)] =
        {"INDEX_ERROR_FOR_INVALID_LAST_HIKEY", "failed for last page's hikey is invalid"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_DELETE_TUPLE_FOR_ALLOCATE_TDID)] =
        {"INDEX_ERROR_FAIL_DELETE_TUPLE_FOR_ALLOCATE_TDID", "delete tuple failed for allocate td id"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_SEARCH_INSERT_FOR_PAGE_DAMAGED)] =
        {"INDEX_ERROR_FAIL_SEARCH_INSERT_FOR_PAGE_DAMAGED", "page {%u, %u} is damaged while read for insert searching"},
    [ERROR_GET_CODE(INDEX_ERROR_INPUT_PARAM_WRONG)] =
        {"INDEX_ERROR_INPUT_PARAM_WRONG", "index input param is wrong"},
    [ERROR_GET_CODE(INDEX_ERROR_MEMORY_ALLOC)] =
        {"INDEX_ERROR_MEMORY_ALLOC", "memory alloc fail"},
    [ERROR_GET_CODE(INDEX_ERROR_UNMATCHED_CCINDEX_STATUS)] =
        {"INDEX_ERROR_UNMATCHED_CCINDEX_STATUS", "ccindex status is %hhu while %hhu expected"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_FOR_INVALID_HEAP_CTID)] =
        {"INDEX_ERROR_FAIL_FOR_INVALID_HEAP_CTID", "invalid heapCtid read from dml table whether is null: %d"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_FOR_INVALID_DML_TYPE)] =
        {"INDEX_ERROR_FAIL_FOR_INVALID_DML_TYPE", "invalid dml operation type {%hhu}"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_BUILD_CONCURRENTLY)] =
        {"INDEX_ERROR_FAIL_BUILD_CONCURRENTLY", "failed to build index"},
    [ERROR_GET_CODE(INDEX_ERROR_INVALID_WAITING_TYPE)] =
        {"INDEX_ERROR_INVALID_WAITING_TYPE", "invalid WaitForTrxsEndType value(%hhu)"},
    [ERROR_GET_CODE(INDEX_ERROR_DUPLICATE_TO_THE_SAME_RECORD)] =
        {"INDEX_ERROR_DUPLICATE_TO_THE_SAME_RECORD", "duplicates point to the same heap record"},
    [ERROR_GET_CODE(INDEX_ERROR_EXPRESSION_VALUE_ERR)] =
        {"INDEX_ERROR_EXPRESSION_VALUE_ERR", "get expression value error"},
    [ERROR_GET_CODE(INDEX_ERROR_UNEXPECTED_NULL_VALUE)] =
        {"INDEX_ERROR_UNEXPECTED_NULL_VALUE", "unexpected nullptr"},
    [ERROR_GET_CODE(INDEX_ERROR_BTREE_STRUCTURE_DAMAGED)] =
        {"INDEX_ERROR_BTREE_STRUCTURE_DAMAGED", "btree structure is damaged"},
    [ERROR_GET_CODE(INDEX_ERROR_FAIL_READ_PAGE)] =
        {"INDEX_ERROR_FAIL_READ_PAGE", "Read index page failed."},
    [ERROR_GET_CODE(INDEX_ERROR_PAGE_DIFF_IN_BUFTAG)] =
        {"INDEX_ERROR_PAGE_DIFF_IN_BUFTAG", "Page {%d, %u} was requested but {%d, %u} returned in bufTag, pdbId:%u."},
    [ERROR_GET_CODE(INDEX_ERROR_PAGE_EMPTY)] =
        {"INDEX_ERROR_PAGE_EMPTY", "Page in bufferDesc {%u, %hu, %u} is empty, pdbId:%u."},
    [ERROR_GET_CODE(INDEX_ERROR_PAGE_DIFF_IN_HEADER)] =
        {"INDEX_ERROR_PAGE_DIFF_IN_HEADER", "Page {%d, %u} was requested but {%d, %u} returned in header, pdbId:%u."},
    [ERROR_GET_CODE(INDEX_ERROR_PAGE_IS_NOT_BTRPAGE)] =
        {"INDEX_ERROR_PAGE_IS_NOT_BTRPAGE", "Page {%d, %u} is not BtrPage, pdbId:%u."},
    [ERROR_GET_CODE(INDEX_ERROR_PAGE_IS_NOT_INITIALIZED_AS_BTRPAGE)] =
        {"INDEX_ERROR_PAGE_IS_NOT_INITIALIZED_AS_BTRPAGE",
         "Page {%d, %u} has not been initialized as BtrPage, pdbId:%u."},
    [ERROR_GET_CODE(INDEX_ERROR_TUPLE_DAMAGED)] =
        {"INDEX_ERROR_TUPLE_DAMAGED", "index tuple is damaged"},
    [ERROR_GET_CODE(INDEX_ERROR_LOW_IS_GREATER_THAN_MAX_OFFSET)] =
        {"INDEX_ERROR_LOW_IS_GREATER_THAN_MAX_OFFSET", "Failed to search on leaf {%hu, %u}."},
    [ERROR_GET_CODE(INDEX_ERROR_FAILED_UPDATE_BTRMETA_ROOT)] =
        {"INDEX_ERROR_FAILED_UPDATE_BTRMETA_ROOT", "Failed to update btrmeta root page id."},
    [ERROR_GET_CODE(INDEX_ERROR_FAILED_TO_BROADCAST_WAITING_MESSAGE)] =
        {"INDEX_ERROR_FAILED_TO_BROADCAST_WAITING_MESSAGE",
         "Failed to notify remote nodes to wait for active transactions end."},
    [ERROR_GET_CODE(INDEX_ERROR_FAILED_TO_GET_SORTED_TUPLE)] =
        {"INDEX_ERROR_FAILED_TO_GET_SORTED_TUPLE", "Failed to get sorted tuple."},
    [ERROR_GET_CODE(INDEX_ERROR_UNCOMMITTED_TUPLE_FOUND_WHEN_BUILDING)] =
        {"INDEX_ERROR_UNCOMMITTED_TUPLE_FOUND_WHEN_BUILDING", "Found uncommitted tuple when building index."},
    [ERROR_GET_CODE(INDEX_ERROR_INVALID_SNAPSHOT_FOR_INDEX_BUILDING)] =
        {"INDEX_ERROR_INVALID_SNAPSHOT_FOR_INDEX_BUILDING", "Snapshot for index building is invalid"},
};

static_assert(sizeof(g_index_error_code_map) != INDEX_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
