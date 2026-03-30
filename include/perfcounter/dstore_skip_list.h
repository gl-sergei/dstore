/* -------------------------------------------------------------------------
 *
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
 * dstore_skip_list.h
 *
 * skiplist is a thread-safety probabilistic data structure that allows search complexity
 * as well as insertion complexity within an ordered sequence of elements.
 * Thus it can get the best features of a sorted array (for searching) while
 * maintaining a linked list-like structure that allows insertion,
 * which is not possible with a static array.
 *
 * IDENTIFICATION
 * container/dstore_skip_list.h
 *
 * Construtor
 *
 * MakeSkipList<TypeKey, TypeValue>(errCode, memCtxPtr, ...)
 * Don't call new() directly for construct a skiplist object. Replace by calling MakeSkipList().
 *
 * Interface introduction (Interface 1 ~ 16 is thread-safety)
 *
 * ********************* Disable duplicate key mode (Begin) *********************
 * 1. insert(*errCode, const &key, const &value) -- inserts a K/V element into the skip list.
 *                                               -- Duplicate key is not allowed.
 *    errCode returns: SUCCESS, KEY_ALREADY_EXIST or OUT_OF_MEMORY.
 *
 * 2. InsertOrAssign(*errCode, const &key, const &value) -- inserts a K/V element into the skip list or assigns
 *                                                         -- to the current element if the key already exists.
 *    errCode returns: SUCCESS, UPDATED_ON_CONFLICT or OUT_OF_MEMORY.
 *
 * 3. find(*errCode, const &key, *value, autoRdLock = true) -- finds value with specific key.
 *    errCode returns: SUCCESS, KEY_NOT_FOUND.
 *
 * 4. GetValueAddress(*errCode, const &key, **value, autoRdLock = true) -- finds the value's addr with specific key.
 *    errCode returns: SUCCESS, KEY_NOT_FOUND.
 *
 * 5. erase(*errCode, const &key, autoRdLock = true) -- erases a K/V element with specific key.
 *    errCode returns: SUCCESS, KEY_NOT_FOUND.
 * ********************* Disable duplicate key mode (End) *********************
 *
 * ********************* Enable duplicate key mode (Begin) *********************
 * 6. insert(*errCode, const &key, value) -- inserts a K/V element into the skip list. Duplicate key is allowed.
 *    errCode returns: SUCCESS or OUT_OF_MEMORY.
 *
 * 7. equal_range(*errCode, const &key, *valuePair, bool autoRdLock = true) -- finds all nodes with specific key.
 *    errCode returns: SUCCESS, KEY_NOT_FOUND.
 *    valuePair is std::pair<SkipList<Key, Value>::iterator, SkipList<Key, Value>::iterator>
 *    valuePair.first is the iterator which point to the first target node.
 *    valuePair.second is the iterator which point to (the next node of end target node).
 *
 * 8. EraseRange(*errCode, const &key, *deleteNum, bool autoWrLock = true) -- erase all nodes with specific key.
 *    errCode returns: SUCCESS, KEY_NOT_FOUND.
 * ********************* Enable duplicate key mode (End) *********************
 *
 * ********************* Common *********************
 * 9. clear() -- erases all elements.
 *
 * 10. erase(*errCode, iter, bool autoWrLock = true)
 *     -- erases a K/V element with specific iterator.
 *     errCode returns: SUCCESS, NO_TARGET_NODE.
 *
 * 11. FindGtFloor(*errCode, const &key, *valuePair, bool autoRdLock = true)
 *     -- finds smallest nodes greater than specific key (not include).
 *     errCode returns: SUCCESS, NO_TARGET_NODE
 *
 * 12. FindItCeil(*errCode, const &key, *valuePair, bool autoRdLock = true)
 *     -- finds largest nodes less than specific key (not include).
 *     errCode returns: SUCCESS, NO_TARGET_NODE
 *
 * 13. FindInRange(*errCode, const &floorKey, const &ceilKey, *valuePair, bool autoRdLock = true)
 *     -- finds nodes in range[floorKey, ceilKey]. (include floor & ceil key)
 *     errCode returns: SUCCESS, NO_TARGET_NODE
 *
 * 14. size(void) -- returns the number of elements.
 *
 * 15. GetIndexLevel(void) -- returns the index level number of the skip list.
 *
 * 16. GetTotalMemory(void) -- returns the memory used for storage k/V elements.
 *
 * 17. The Iterator for skip list.
 * It is not thread-safety! We must call AcquireSharedLockForIterator() before use iterator
 * and calling ReleaseSharedLockForIterator() to release lock.
 * -------------------------------------------------------------------------
 */
#ifndef DSTORE_SKIP_LIST_H
#define DSTORE_SKIP_LIST_H

#include <atomic>
#include <iostream>
#include <pthread.h>
#include <random>

namespace DSTORE {
enum class SkipListErrNo {
    SUCCESS = 0,
    UPDATED_ON_CONFLICT,
    KEY_ALREADY_EXIST,
    KEY_NOT_FOUND,
    OUT_OF_MEMORY,
    INVALID_MAX_LEVEL,
    NO_TARGET_NODE,
    DUPLICATE_KEY_NOT_SUPPORT
};

constexpr int32 SKIPLIST_DEFAULT_MAX_LEVEL = 32;
constexpr int32 SKIPLIST_MIN_LEVEL = 2;
constexpr int32 SKIPLIST_MAX_LEVEL = 128;
constexpr int32 SKIPLIST_INDEX_INIT_LEVEL = -1;
constexpr int32 SKIPLIST_WHOLE_DATA_LEVEL = 0;

template <typename Key, typename Value>
class SkipList {
public:
    struct SkipListStatus {
        std::atomic_uint64_t totalMemory;
        std::atomic_uint32_t nodeSize;
        std::atomic_uint32_t retryTimes;
    };
    using SkipListKeyCmpFunc = int32 (*)(const Key &key1, const Key &key2);

    /* Don't call this private constuctor directly. Replace by calling MakeSkipList(). */
    explicit SkipList(DstoreMemoryContext memCtxPtr, SkipListKeyCmpFunc cmpFunc = nullptr,
                      bool enableDuplicateKey = false, int32 maxLevel = SKIPLIST_DEFAULT_MAX_LEVEL)
        : m_memCtxPtr(memCtxPtr),
          m_keyCmpFunc(cmpFunc == nullptr ? SkipListDefaultKeyCmpFunc : cmpFunc),
          m_enableDuplicateKey(enableDuplicateKey),
          m_maxLevel(maxLevel)

    {
        m_indexLevel.store(SKIPLIST_INDEX_INIT_LEVEL);
        m_status.nodeSize.store(0);
        m_status.totalMemory.store(0);
        m_status.retryTimes.store(0);
    }

    SkipListErrNo Init()
    {
        if (unlikely(m_maxLevel < SKIPLIST_MIN_LEVEL || m_maxLevel > SKIPLIST_MAX_LEVEL)) {
            return SkipListErrNo::INVALID_MAX_LEVEL;
        }

        /* Seeds the random number generator used by SkipListGetRandomIndexLevel(). */
        srand(static_cast<uint32>(time(nullptr)));
        ASSERT(m_header == nullptr);
        m_header = CreateHeaderNode();
        if (unlikely(m_header == nullptr)) {
            return SkipListErrNo::OUT_OF_MEMORY;
        }
        return SkipListErrNo::SUCCESS;
    }

    ~SkipList()
    {
        if (unlikely(m_maxLevel < SKIPLIST_MIN_LEVEL || m_maxLevel > SKIPLIST_MAX_LEVEL)) {
            return;
        }

        while (m_header != nullptr) {
            SkipListNode *cursor = m_header;
            m_header = m_header->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
            DestorySkipListNode(cursor);
        }
    }

    DISALLOW_COPY_AND_MOVE(SkipList);

    /*
     * inserts a K/V element into the skip list.
     * errCode returns: SUCCESS, KEY_ALREADY_EXIST or OUT_OF_MEMORY.
     */
    void Insert(SkipListErrNo *errCode, const Key &key, const Value &value, UNUSE_PARAM bool autoRdLock = true)
    {
        SkipListNode *newNode = CreateSkipListNode(key, value);
        if (unlikely(newNode == nullptr)) {
            *errCode = SkipListErrNo::OUT_OF_MEMORY;
            return;
        }

        *errCode = InsertSkipListNode(newNode, false, nullptr, autoRdLock);
        if (unlikely(*errCode != SkipListErrNo::SUCCESS)) {
            DestorySkipListNode(newNode);
        }
    }

    /*
     * inserts a K/V element into the skip list or assigns to the current element if the key already exists.
     * errCode returns: SUCCESS, UPDATED_ON_CONFLICT or OUT_OF_MEMORY.
     */
    void InsertOrAssign(SkipListErrNo *errCode, const Key &key, const Value &value, Value *oldValue = nullptr,
                        UNUSE_PARAM bool autoRdLock = true)
    {
        if (unlikely(m_enableDuplicateKey)) {
            *errCode = SkipListErrNo::DUPLICATE_KEY_NOT_SUPPORT;
            return;
        }

        SkipListNode *node = CreateSkipListNode(key, value);
        if (unlikely(node == nullptr)) {
            *errCode = SkipListErrNo::OUT_OF_MEMORY;
            return;
        }

        *errCode = InsertSkipListNode(node, true, oldValue, autoRdLock);
        if (*errCode != SkipListErrNo::SUCCESS) {
            DestorySkipListNode(node);
        }
    }

    /*
     * finds value with specific key.
     * errCode returns: SUCCESS, KEY_NOT_FOUND.
     */
    void Find(SkipListErrNo *errCode, const Key &key, Value *value, UNUSE_PARAM bool autoRdLock = true)
    {
        if (unlikely(m_enableDuplicateKey)) {
            *errCode = SkipListErrNo::DUPLICATE_KEY_NOT_SUPPORT;
            return;
        }

        SkipListNode *cursor = m_header;
        LockShare();
        for (int curLevel = m_indexLevel.load(); curLevel >= 0; curLevel--) {
            while (true) {
                SkipListNode *nextNode = cursor->nextInLevel[curLevel].load();
                if (nextNode == nullptr) {
                    break;
                }
                int32 cmp = m_keyCmpFunc(key, nextNode->key);
                if (cmp < 0) {
                    break;
                } else if (cmp > 0) {
                    cursor = cursor->nextInLevel[curLevel].load();
                } else {
                    *value = nextNode->value;
                    UnlockShare();
                    *errCode = SkipListErrNo::SUCCESS;
                    return;
                }
            }
        }
        UnlockShare();
        *errCode = SkipListErrNo::KEY_NOT_FOUND;
    }

    /*
     * return the value address with specific key.
     * errCode returns: SUCCESS, KEY_NOT_FOUND.
     */
    void GetValueAddress(SkipListErrNo *errCode, const Key &key, Value **value, UNUSE_PARAM bool autoRdLock = true)
    {
        if (unlikely(m_enableDuplicateKey)) {
            *errCode = SkipListErrNo::DUPLICATE_KEY_NOT_SUPPORT;
            return;
        }

        SkipListNode *cursor = m_header;
        LockShare();
        for (int level = m_indexLevel.load(); level >= 0; level--) {
            while (true) {
                SkipListNode *nextNode = cursor->nextInLevel[level].load();
                if (nextNode == nullptr) {
                    break;
                }
                int32 cmp = m_keyCmpFunc(key, nextNode->key);
                if (cmp < 0) {
                    break;
                } else if (cmp > 0) {
                    cursor = cursor->nextInLevel[level].load();
                } else {
                    /* value may be freed by others after release lock. */
                    *value = &(nextNode->value);
                    UnlockShare();
                    *errCode = SkipListErrNo::SUCCESS;
                    return;
                }
            }
        }
        UnlockShare();
        *value = nullptr;
        *errCode = SkipListErrNo::KEY_NOT_FOUND;
    }

    /*
     * erases a K/V element with specific key.
     * errCode returns: SUCCESS, KEY_NOT_FOUND.
     */
    void Erase(SkipListErrNo *errCode, const Key &key, UNUSE_PARAM bool autoWrLock = true, Value *curValue = nullptr)
    {
        if (unlikely(m_enableDuplicateKey)) {
            *errCode = SkipListErrNo::DUPLICATE_KEY_NOT_SUPPORT;
            return;
        }

        SkipListNode *cursor = m_header;
        SkipListNode *deleteNode = nullptr;
        SkipListNode *targetPrev[SKIPLIST_MAX_LEVEL];

        for (int curLevel = m_indexLevel.load(); curLevel >= 0; curLevel--) {
            SkipListNode *firstGENode = cursor->nextInLevel[curLevel].load();
            while ((cursor->nextInLevel[curLevel].load() != nullptr) &&
                   (m_keyCmpFunc(key, cursor->nextInLevel[curLevel].load()->key) > 0)) {
                cursor = cursor->nextInLevel[curLevel].load();
                firstGENode = cursor->nextInLevel[curLevel].load();
            }

            /*
             * If the target node appears in a certain level, it will also appear in the lower level.
             */
            if (deleteNode != nullptr) {
                ASSERT(firstGENode == deleteNode);
                targetPrev[curLevel] = cursor;
            } else if ((firstGENode != nullptr) && (m_keyCmpFunc(key, firstGENode->key) == 0)) {
                deleteNode = firstGENode;
                ASSERT(deleteNode->topIndexLevel == curLevel);
                targetPrev[curLevel] = cursor;
            }
        }

        if (likely(deleteNode != nullptr)) {
            if (curValue != nullptr) {
                *curValue = deleteNode->value;
            }
            RemoveTargetNode(deleteNode, targetPrev);
            DestorySkipListNode(deleteNode);
            *errCode = SkipListErrNo::SUCCESS;
        } else {
            *errCode = SkipListErrNo::KEY_NOT_FOUND;
        }
    }

    /*
     * erases all elements.
     */
    void Clear()
    {
        SkipListNode *cursor = nullptr;

        DstoreLWLockAcquire(&m_rwlock, LW_EXCLUSIVE);
        cursor = m_header->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
        for (int i = 0; i <= m_header->topIndexLevel; i++) {
            m_header->nextInLevel[i].store(nullptr);
        }
        m_indexLevel.store(SKIPLIST_INDEX_INIT_LEVEL);
        m_status.nodeSize.store(0);
        m_status.totalMemory.store(0);
        m_status.retryTimes.store(0);
        LWLockRelease(&m_rwlock);

        while (cursor != nullptr) {
            SkipListNode *tmpCursor = cursor;
            cursor = cursor->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
            DestorySkipListNode(tmpCursor);
        }
    }

    uint32 Size(void) const
    {
        return m_status.nodeSize.load();
    }

    int32 GetIndexLevel(void) const
    {
        return m_indexLevel.load();
    }

    uint64 GetTotalMemory(void) const
    {
        return m_status.totalMemory.load();
    }

    void LockShare(void)
    {
        DstoreLWLockAcquire(&m_rwlock, LW_SHARED);
    }

    void LockExclusive(void)
    {
        DstoreLWLockAcquire(&m_rwlock, LW_EXCLUSIVE);
    }

    /*
     * Release shared lock.
     */
    void UnlockShare(void)
    {
        LWLockRelease(&m_rwlock);
    }

    /*
     * Release Exclusive lock.
     */
    void UnlockExclusive(void)
    {
        LWLockRelease(&m_rwlock);
    }

    static size_t GetItemMaxSize(int32 maxLevel = SKIPLIST_DEFAULT_MAX_LEVEL)
    {
        /* node max level * ptr size + SkipListNode.topIndexLevel + sizeof key + sizeof value */
        return static_cast<uint32>(maxLevel) * sizeof(SkipListNode *) + sizeof(int32) + sizeof(Key) + sizeof(Value);
    }

private:
    struct SkipListNode {
        Key key{};
        Value value{};
        int32 topIndexLevel{0};
        std::atomic<SkipListNode *> *nextInLevel{nullptr};

        SkipListNode() = default;
        SkipListNode(Key nodeKey, Value nodeValue, int32 level)
            : key(nodeKey), value(nodeValue), topIndexLevel(level), nextInLevel(nullptr){};
        DISALLOW_COPY_AND_MOVE(SkipListNode);
    };

    struct SkipListLevelExtent {
        int32 minLevel;
        int32 maxLevel;
    };

    struct SkipListInsertSnapshot {
        SkipListNode **snapshotPrev;
        SkipListNode **snapshotNext;
    };

    int SkipListGetRandomIndexLevel()
    {
        int indexLevel = 0;

        while ((rand() & 1) && (indexLevel < (m_maxLevel - 1))) {
            indexLevel++;
        }
        return indexLevel;
    }

    static int32 SkipListDefaultKeyCmpFunc(const Key &key1, const Key &key2)
    {
        if (key1 < key2) {
            return -1;
        } else if (key2 < key1) {
            return 1;
        } else {
            return 0;
        }
    }

    inline void AcquireSkipRdLock(bool autoLock)
    {
        if (autoLock) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("PerfCounter rd Lock."));
            DstoreLWLockAcquire(&m_rwlock, LW_SHARED);
        }
    }

    inline void AcquireSkipWrLock(bool autoLock)
    {
        if (autoLock) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("PerfCounter Wr Lock."));
            DstoreLWLockAcquire(&m_rwlock, LW_EXCLUSIVE);
        }
    }

    inline void ReleaseSkipLock(bool autoLock)
    {
        if (autoLock) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("PerfCounter Release Lock."));
            LWLockRelease(&m_rwlock);
        }
    }

    SkipListNode *CreateHeaderNode()
    {
        SkipListNode *node = NEW(m_memCtxPtr) SkipListNode();
        if (unlikely(node == nullptr)) {
            return nullptr;
        }

        node->topIndexLevel = m_maxLevel - 1;
        node->nextInLevel = NEW(m_memCtxPtr) std::atomic<SkipListNode *>[node->topIndexLevel + 1];
        if (unlikely(node->nextInLevel == nullptr)) {
            DELETE(node);
            return nullptr;
        }

        for (int i = 0; i <= node->topIndexLevel; i++) {
            node->nextInLevel[i].store(nullptr);
        }
        return node;
    }

    SkipListNode *CreateSkipListNode(Key key, Value value)
    {
        SkipListNode *node = NEW(m_memCtxPtr) SkipListNode(key, value, SkipListGetRandomIndexLevel());
        if (unlikely(node == nullptr)) {
            return nullptr;
        }

        node->nextInLevel = NEW(m_memCtxPtr) std::atomic<SkipListNode *>[node->topIndexLevel + 1];
        if (unlikely(node->nextInLevel == nullptr)) {
            DELETE(node);
            return nullptr;
        }

        return node;
    }

    void DestorySkipListNode(SkipListNode *&targetNode) noexcept
    {
        if (unlikely(targetNode == nullptr)) {
            return;
        }
        DELETE_ARRAY_AND_RESET(targetNode->nextInLevel);
        DELETE_AND_RESET(targetNode);
    }

    SkipListErrNo InsertSkipListNode(SkipListNode *newNode, bool updateOnConflict, Value *oldValue,
                                     UNUSE_PARAM bool autoRdLock = true)
    {
        int32 topIndexLevel = newNode->topIndexLevel;
        SkipListLevelExtent levelExtent = {SKIPLIST_WHOLE_DATA_LEVEL, topIndexLevel};
        SkipListInsertSnapshot snapshot = {nullptr, nullptr};
        snapshot.snapshotPrev = NEW(m_memCtxPtr) SkipListNode * [(newNode->topIndexLevel + 1)];
        if (unlikely(snapshot.snapshotPrev == nullptr)) {
            return SkipListErrNo::OUT_OF_MEMORY;
        }
        snapshot.snapshotNext = NEW(m_memCtxPtr) SkipListNode * [(newNode->topIndexLevel + 1)];
        if (unlikely(snapshot.snapshotNext == nullptr)) {
            DELETE_ARRAY(snapshot.snapshotPrev);
            return SkipListErrNo::OUT_OF_MEMORY;
        }

        LockShare();
        SkipListErrNo errCode = TryToGetSnapshot(newNode, levelExtent, snapshot, updateOnConflict, oldValue);
        if (errCode != SkipListErrNo::SUCCESS) {
            UnlockShare();
            DELETE_ARRAY(snapshot.snapshotPrev);
            DELETE_ARRAY(snapshot.snapshotNext);
            return errCode;
        }

        for (int curLevel = 0; curLevel <= topIndexLevel; curLevel++) {
            while (true) {
                newNode->nextInLevel[curLevel].store(snapshot.snapshotNext[curLevel]);
                if (snapshot.snapshotPrev[curLevel]->nextInLevel[curLevel].compare_exchange_weak(
                    snapshot.snapshotNext[curLevel], newNode)) {
                    break;
                }
                (void)m_status.retryTimes.fetch_add(1);
                levelExtent.minLevel = curLevel;
                errCode = TryToGetSnapshot(newNode, levelExtent, snapshot, updateOnConflict, oldValue);
                if (errCode != SkipListErrNo::SUCCESS) {
                    UnlockShare();
                    DELETE_ARRAY(snapshot.snapshotPrev);
                    DELETE_ARRAY(snapshot.snapshotNext);
                    return errCode;
                }
            }
        }

        /* Update skiplist index level if needed. It must occurred after that new node has been inserted completely. */
        int indexLevel = m_indexLevel.load();
        while (indexLevel < topIndexLevel && !(m_indexLevel.compare_exchange_weak(indexLevel, topIndexLevel))) {
        }

        (void)m_status.totalMemory.fetch_add(GetSkipListNodeSize(newNode));
        (void)m_status.nodeSize.fetch_add(1);
        UnlockShare();

        DELETE_ARRAY(snapshot.snapshotPrev);
        DELETE_ARRAY(snapshot.snapshotNext);
        return SkipListErrNo::SUCCESS;
    }

    SkipListErrNo TryToGetSnapshot(SkipListNode *newNode, const SkipListLevelExtent &levelExtent,
                                   SkipListInsertSnapshot &snapshot, bool updateOnConflict, Value *oldValue)
    {
        SkipListErrNo errCode;

        if (updateOnConflict) {
            Value *valuePtr = nullptr;
            errCode = GetInsertSnapshot(newNode->key, levelExtent, snapshot, true, &valuePtr);
            if (errCode == SkipListErrNo::KEY_ALREADY_EXIST) {
                if (oldValue != nullptr) {
                    *oldValue = *valuePtr;
                }
                *valuePtr = newNode->value;
                return SkipListErrNo::UPDATED_ON_CONFLICT;
            }
        } else {
            errCode = GetInsertSnapshot(newNode->key, levelExtent, snapshot, false, nullptr);
        }
        return errCode;
    }

    /*
     * Get the insert postion's snapshot on each level between levelExtent.
     *
     * GetInsertSnapshot() confirms the node insert position on each level list currently, and storage
     * forward node(> key) & backward nodes(< key) on snapshot array.
     * It cannot ensure that snaphot keep abreast of the times, because we held shared lock only and another insert
     * operation can established at this moment. It doesn't matter, we will check snapshot during inserting.
     * If the snapshot is expired, we will call GetInsertSnapshot() afresh.
     * If the snapshot is not expired, new node can be safely inserted because the insertion interval has been "locked"
     * by us. Others in the back will see and use the new node, or their snapshots are invalidated.
     */
    SkipListErrNo GetInsertSnapshot(const Key &key, const SkipListLevelExtent &levelExtent,
                                    SkipListInsertSnapshot &snapshot, bool updateOnConflict, Value **newValue)
    {
        SkipListNode *cursor = m_header;
        int curLevel = m_indexLevel.load();
        int nodeIndexLevel = levelExtent.maxLevel;
        int cmpval = (m_enableDuplicateKey ? 0 : 1);

        /*
         * Maybe newNode's level is greater than skiplist's level, owing to skiplist node is a probabilistic data
         * structure.
         * This operation may get a snapshot of a hierarchy that is not the latest, because a new higher-level node is
         * being inserted at this time, which will result in a snapshot that is not the latest. It doesn't matter,
         * we will notice that and get the latest snapshot of the insertion interval when inserting node later.
         */
        while (nodeIndexLevel > curLevel) {
            snapshot.snapshotPrev[nodeIndexLevel] = m_header;
            snapshot.snapshotNext[nodeIndexLevel] = nullptr;
            nodeIndexLevel--;
        }

        for (; curLevel >= levelExtent.minLevel; curLevel--) {
            if (nodeIndexLevel == curLevel) {
                snapshot.snapshotNext[curLevel] = cursor->nextInLevel[curLevel].load();
            }
            /*
             * Get the snapshot of new node insert position.
             * snapshotPrev: the backward node of insert postion on the snapshot.
             * snapshotNext: the forward node of insert position on the snapshot.
             *
             * Analysis of the influence of snapshotPrev forward pointer Changes at the parallel insertion condition
             * Command 1 (cursor->nextInLevel[curLevel].load() != nullptr):
             *     After confirm that snapshotPrev's forward node is not empty, if the forward node changes when the
             *     subsequent operation is performed, the condition that "the new successor node of snapshotPrev
             *     is not empty" still holds.
             *
             * Command 2 (m_keyCmpFunc(key, cursor->nextInLevel[curLevel].load()->key) > 0)
             *     After confirm that snapshotPrev's forward node is smaller than the target key, if the forward node
             *     changes when the subsequent operation is performed, the condition that "the new forward node of
             *     snapshotPrev is smaller than the key" still holds, because the list is ordered.
             *
             * Command 3 (cursor = cursor->nextInLevel[curLevel].load())
             *     After snapshotPrev is updated, if the forward node changes during subsequent operations, a more
             *     accurate forward node will be used for judgment.
             *
             * Command 4 (snapshot.snapshotNext[curLevel] = cursor->nextInLevel[curLevel].load())
             *     After snapshotNext saves the forward node, if the forward node changes during subsequent operations:
             *         Condition (1): (snapshotNext >= key) and (cursor's new forward node < key), then continue to
             *                        enter the scan list to avoid a snapshot failure.
             *         Condition (2): (snapshotNext >= key) and (cursor's new forward node >= key), it will be found
             *                        that the snapshot has expired when insert node, and we will get the latest
             *                        snapshot of the insertion interval.
             *         Condition (3): if condition (snapshotNext < key), cursor's new forward node must < key, it will
             *                        continue to enter scan list.
             */
            while ((cursor->nextInLevel[curLevel].load() != nullptr) &&
                   (m_keyCmpFunc(key, cursor->nextInLevel[curLevel].load()->key) >= cmpval)) {
                cursor = cursor->nextInLevel[curLevel].load();
                if (nodeIndexLevel == curLevel) {
                    snapshot.snapshotNext[curLevel] = cursor->nextInLevel[curLevel].load();
                }
            }

            /*
             * If there is a target node exist on the current level, an error will must occur, even if the newly
             * inserted node smaller than key is not triggered this time. Because the snapshot has expired,
             * the latest snapshot will be obtained in the future.
             */
            SkipListNode *cursorNext = cursor->nextInLevel[curLevel].load();
            if (!m_enableDuplicateKey && ((cursorNext != nullptr) && (m_keyCmpFunc(key, cursorNext->key) == 0))) {
                if (updateOnConflict) {
                    *newValue = &(cursorNext->value);
                }
                return SkipListErrNo::KEY_ALREADY_EXIST;
            }

            if (nodeIndexLevel == curLevel) {
                snapshot.snapshotPrev[nodeIndexLevel] = cursor;
                nodeIndexLevel--;
            }
        }
        return SkipListErrNo::SUCCESS;
    }

    void RemoveTargetNode(SkipListNode *deleteNode, SkipListNode **targetPrev)
    {
        int deleteNodeLevel = deleteNode->topIndexLevel;

        while (deleteNodeLevel >= 0) {
            targetPrev[deleteNodeLevel]->nextInLevel[deleteNodeLevel].store(deleteNode->nextInLevel[deleteNodeLevel]);
            /* Case: m_header->next == nullptr */
            if (unlikely(targetPrev[deleteNodeLevel] == m_header &&
                         targetPrev[deleteNodeLevel]->nextInLevel[deleteNodeLevel] == nullptr)) {
                (void)m_indexLevel.fetch_sub(1);
            }
            deleteNodeLevel--;
        }

        (void)m_status.nodeSize.fetch_sub(1);
        (void)m_status.totalMemory.fetch_sub(GetSkipListNodeSize(deleteNode));
    }

    int32 RemoveMultiTargetNodes(Key key, SkipListNode *deleteOrigin, SkipListNode **targetPrev, int32 appearMaxLevel)
    {
        SkipListNode *cursor = nullptr;
        SkipListNode *deleteEnd = deleteOrigin;
        SkipListNode *targetNext[SKIPLIST_MAX_LEVEL];
        int32 deleteNodeNum = 0;

        while (deleteEnd->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load() != nullptr &&
               m_keyCmpFunc(key, deleteEnd->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load()->key) == 0) {
            deleteEnd = deleteEnd->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
        }

        while (appearMaxLevel >= 0) {
            for (cursor = deleteOrigin; cursor != deleteEnd->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
                 cursor = cursor->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load()) {
                if (cursor->topIndexLevel >= appearMaxLevel) {
                    targetNext[appearMaxLevel] = cursor->nextInLevel[appearMaxLevel].load();
                }
            }

            targetPrev[appearMaxLevel]->nextInLevel[appearMaxLevel].store(targetNext[appearMaxLevel]);

            /* Case: m_header->next == nullptr */
            if (unlikely(targetPrev[appearMaxLevel] == m_header &&
                         targetPrev[appearMaxLevel]->nextInLevel[appearMaxLevel] == nullptr)) {
                m_indexLevel.fetch_sub(1);
            }

            appearMaxLevel--;
        }

        cursor = deleteOrigin;
        SkipListNode *deleteEndNext = deleteEnd->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
        while (cursor != deleteEndNext) {
            SkipListNode *tmpCursor = cursor;
            cursor = cursor->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
            m_status.nodeSize.fetch_sub(1);
            m_status.totalMemory.fetch_sub(GetSkipListNodeSize(tmpCursor));
            DestorySkipListNode(tmpCursor);
            deleteNodeNum++;
        }
        return deleteNodeNum;
    }

    void RouteToDataLevel(SkipListNode *&cursor, SkipListNode *&nextNode, const Key &key, bool isFindGreater = false)
    {
        cursor = m_header;
        for (int curLevel = m_indexLevel.load(); curLevel >= 0; curLevel--) {
            while (true) {
                nextNode = cursor->nextInLevel[curLevel].load();
                if (nextNode == nullptr) {
                    break;
                }

                int32 cmp = m_keyCmpFunc(key, nextNode->key);
                if (cmp > 0) {
                    cursor = cursor->nextInLevel[curLevel].load();
                } else if (cmp < 0) {
                    break;
                    /* Condition: cmp == 0 */
                } else if (!isFindGreater) {
                    break;
                } else {
                    cursor = cursor->nextInLevel[curLevel].load();
                }
            }
        }
    }

    inline uint32 GetNextPtrRecorderSize(SkipListNode *node)
    {
        return static_cast<uint32>(static_cast<uint32>(node->topIndexLevel + 1) * sizeof(std::atomic<SkipListNode *>));
    }

    inline uint32 GetSkipListNodeSize(SkipListNode *node)
    {
        return static_cast<uint32>(sizeof(SkipListNode) + GetNextPtrRecorderSize(node));
    }

    SkipListNode *m_header{nullptr};
    std::atomic<int32> m_indexLevel{SKIPLIST_INDEX_INIT_LEVEL};

    LWLock m_rwlock{};
    DstoreMemoryContext m_memCtxPtr{nullptr};
    SkipListKeyCmpFunc m_keyCmpFunc{SkipListDefaultKeyCmpFunc};
    bool m_enableDuplicateKey{false};
    int32 m_maxLevel{SKIPLIST_DEFAULT_MAX_LEVEL};
    SkipListStatus m_status{};

public:
    struct Iterator {
        SkipListNode *cursor;

        explicit Iterator() : cursor(nullptr)
        {}

        explicit Iterator(SkipListNode *node) : cursor(node)
        {}

        Iterator &operator++()
        {
            cursor = cursor->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
            return *this;
        }

        const Iterator operator++(int)
        {
            Iterator tmp = *this;
            ++*this;
            return tmp;
        }

        bool operator!=(const Iterator &comp) const
        {
            return cursor != comp.cursor;
        }

        bool operator==(const Iterator &comp) const
        {
            return cursor == comp.cursor;
        }

        Key key() const
        {
            ASSERT(cursor != nullptr);
            return cursor->key;
        }

        Value &value() const
        {
            ASSERT(cursor != nullptr);
            return cursor->value;
        }
    };

    void AcquireSharedLockForIterator()
    {
        DstoreLWLockAcquire(&m_rwlock, LW_SHARED);
    }

    void ReleaseSharedLockForIterator()
    {
        LWLockRelease(&m_rwlock);
    }

    Iterator Begin() const
    {
        ASSERT(m_header != nullptr);
        return Iterator(m_header->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load());
    }

    Iterator End() const
    {
        return Iterator(nullptr);
    }

    /*
     * erases a K/V element with specific iterator.
     * errCode returns: SUCCESS, NO_TARGET_NODE.
     */
    void Erase(SkipListErrNo *errCode, SkipList<Key, Value>::Iterator iter, UNUSE_PARAM bool autoWrLock = true)
    {
        if (unlikely(iter.cursor == nullptr)) {
            *errCode = SkipListErrNo::NO_TARGET_NODE;
            return;
        }

        SkipListNode *cursor = m_header;
        SkipListNode *targetPrev[SKIPLIST_MAX_LEVEL];

        for (int32 curLevel = m_indexLevel.load(); curLevel >= iter.cursor->topIndexLevel; curLevel--) {
            while ((cursor->nextInLevel[curLevel].load() != nullptr) &&
                   (m_keyCmpFunc(iter.cursor->key, cursor->nextInLevel[curLevel].load()->key) > 0)) {
                cursor = cursor->nextInLevel[curLevel].load();
            }
        }

        for (int32 curLevel = iter.cursor->topIndexLevel; curLevel >= 0; curLevel--) {
            while (cursor->nextInLevel[curLevel].load() != iter.cursor) {
                ASSERT(cursor->nextInLevel[curLevel].load()->key <= iter.cursor->key);
                cursor = cursor->nextInLevel[curLevel].load();
            }
            targetPrev[curLevel] = cursor;
        }

        RemoveTargetNode(iter.cursor, targetPrev);
        DestorySkipListNode(iter.cursor);
        *errCode = SkipListErrNo::SUCCESS;
    }

    /*
     * finds all nodes with specific key. (enableDuplicateKey mode)
     * errCode returns: SUCCESS, KEY_NOT_FOUND.
     */
    void equal_range(SkipListErrNo *errCode, const Key &key,
                     std::pair<SkipList<Key, Value>::Iterator, SkipList<Key, Value>::Iterator> *valuePair,
                     UNUSE_PARAM bool autoRdLock = true)
    {
        SkipListNode *cursor = nullptr;
        SkipListNode *nextNode = nullptr;
        LockShare();

        RouteToDataLevel(cursor, nextNode, key);

        if (unlikely(nextNode == nullptr || m_keyCmpFunc(key, nextNode->key) < 0)) {
            valuePair->first.cursor = nullptr;
            valuePair->second.cursor = nullptr;
            *errCode = SkipListErrNo::KEY_NOT_FOUND;
        } else {
            valuePair->first.cursor = nextNode;
            do {
                nextNode = nextNode->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
            } while (nextNode != nullptr && m_keyCmpFunc(key, nextNode->key) == 0);
            valuePair->second.cursor = nextNode;
            *errCode = SkipListErrNo::SUCCESS;
        }
        UnlockShare();
    }

    /*
     * erase all nodes with specific key. (enableDuplicateKey mode)
     * errCode returns: SUCCESS, KEY_NOT_FOUND.
     */
    void EraseRange(SkipListErrNo *errCode, const Key &key, uint32 *deleteNum, UNUSE_PARAM bool autoWrLock = true)
    {
        SkipListNode *cursor = m_header;
        SkipListNode *deleteOrigin = nullptr;
        SkipListNode *targetPrev[SKIPLIST_MAX_LEVEL];
        int32 appearMaxLevel = 0;

        LockExclusive();
        for (int32 level = m_indexLevel.load(); level >= 0; level--) {
            SkipListNode *firstGENode = cursor->nextInLevel[level].load();
            while ((cursor->nextInLevel[level].load() != nullptr) &&
                   (m_keyCmpFunc(key, cursor->nextInLevel[level].load()->key) > 0)) {
                cursor = cursor->nextInLevel[level].load();
                firstGENode = cursor->nextInLevel[level].load();
            }

            /*
             * If the target node appears in a certain level, it will also appear in the lower level.
             */
            if (deleteOrigin != nullptr) {
                ASSERT(firstGENode->key == deleteOrigin->key);
                deleteOrigin = firstGENode;
                targetPrev[level] = cursor;
            } else if ((firstGENode != nullptr) && (m_keyCmpFunc(key, firstGENode->key) == 0)) {
                deleteOrigin = firstGENode;
                ASSERT(deleteOrigin->topIndexLevel == level);
                targetPrev[level] = cursor;
                appearMaxLevel = level;
            }
        }

        if (likely(deleteOrigin != nullptr)) {
            *deleteNum = RemoveMultiTargetNodes(key, deleteOrigin, targetPrev, appearMaxLevel);
            *errCode = SkipListErrNo::SUCCESS;
        } else {
            *deleteNum = 0;
            *errCode = SkipListErrNo::KEY_NOT_FOUND;
        }
        UnlockExclusive();
    }

    /*
     * finds smallest nodes which greater than specific key (not include).
     * errCode returns: SUCCESS, NO_TARGET_NODE.
     */
    void find_gt_floor(SkipListErrNo *errCode, const Key &key,
                       std::pair<SkipList<Key, Value>::Iterator, SkipList<Key, Value>::Iterator> *valuePair,
                       UNUSE_PARAM bool autoRdLock = true)
    {
        SkipListNode *cursor = nullptr;
        SkipListNode *nextNode = nullptr;
        LockShare();

        RouteToDataLevel(cursor, nextNode, key, true);

        if (unlikely(nextNode == nullptr)) {
            valuePair->first.cursor = nullptr;
            valuePair->second.cursor = nullptr;
            *errCode = SkipListErrNo::NO_TARGET_NODE;
        } else {
            valuePair->first.cursor = nextNode;
            Key targetKey = nextNode->key;
            do {
                nextNode = nextNode->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
            } while (nextNode != nullptr && m_keyCmpFunc(nextNode->key, targetKey) == 0);
            valuePair->second.cursor = nextNode;
            *errCode = SkipListErrNo::SUCCESS;
        }
        UnlockShare();
    }

    /*
     * finds largest nodes which less than specific key (not include).
     * errCode returns: SUCCESS, NO_TARGET_NODE.
     */
    void find_lt_ceil(SkipListErrNo *errCode, const Key &key,
                      std::pair<SkipList<Key, Value>::Iterator, SkipList<Key, Value>::Iterator> *valuePair,
                      UNUSE_PARAM bool autoRdLock = true)
    {
        SkipListNode *cursor = nullptr;
        SkipListNode *nextNode = nullptr;
        LockShare();

        RouteToDataLevel(cursor, nextNode, key);

        if (unlikely(cursor == m_header)) {
            valuePair->first.cursor = nullptr;
            valuePair->second.cursor = nullptr;
            *errCode = SkipListErrNo::NO_TARGET_NODE;
        } else {
            if (m_enableDuplicateKey) {
                equal_range(errCode, cursor->key, valuePair, false);
            } else {
                valuePair->first.cursor = cursor;
                valuePair->second.cursor = cursor->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
            }
            *errCode = SkipListErrNo::SUCCESS;
        }
        UnlockShare();
    }

    /*
     * finds nodes with in range[floorKey, ceilKey]. (include floor & ceil key)
     * errCode returns: SUCCESS, NO_TARGET_NODE.
     */
    void find_in_range(SkipListErrNo *errCode, const Key &floorkKey, const Key &ceilKey,
                       std::pair<SkipList<Key, Value>::Iterator, SkipList<Key, Value>::Iterator> *valuePair,
                       UNUSE_PARAM bool autoRdLock = true)
    {
        SkipListNode *cursor = nullptr;
        SkipListNode *nextNode = nullptr;
        ASSERT(floorkKey <= ceilKey);

        LockShare();

        RouteToDataLevel(cursor, nextNode, floorkKey);

        if (unlikely(nextNode == nullptr || m_keyCmpFunc(nextNode->key, ceilKey) > 0)) {
            valuePair->first.cursor = nullptr;
            valuePair->second.cursor = nullptr;
            *errCode = SkipListErrNo::NO_TARGET_NODE;
        } else {
            valuePair->first.cursor = nextNode;
            do {
                valuePair->second.cursor = nextNode;
                nextNode = nextNode->nextInLevel[SKIPLIST_WHOLE_DATA_LEVEL].load();
            } while (nextNode != nullptr && m_keyCmpFunc(nextNode->key, ceilKey) <= 0);
            *errCode = SkipListErrNo::SUCCESS;
        }
        UnlockShare();
    }
};

template <typename TypeKey, typename TypeValue, typename... Args>
inline SkipList<TypeKey, TypeValue> *MakeSkipList(SkipListErrNo *errCode, DstoreMemoryContext memCtxPtr, Args... args)
{
    SkipList<TypeKey, TypeValue> *skipList = NEW(memCtxPtr) SkipList<TypeKey, TypeValue>(memCtxPtr, args...);
    if (likely(skipList != nullptr)) {
        *errCode = skipList->Init();
        if (unlikely(*errCode != SkipListErrNo::SUCCESS)) {
            DELETE_AND_RESET(skipList);
        }
    } else {
        *errCode = SkipListErrNo::OUT_OF_MEMORY;
    }

    return skipList;
}
}  // namespace DSTORE
#endif /* STORAGE_SKIP_LIST_H */
