#pragma once
#include "BTreeSlotted.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
enum class WAL_LOG_TYPE : u8 {
   WALInsert = 1,
   WALUpdate = 2,
   WALRemove = 3,
   WALAfterBeforeImage = 4,
   WALAfterImage = 5,
   WALLogicalSplit = 10,
   WALInitPage = 11
};
struct WALEntry {
   WAL_LOG_TYPE type;
};
namespace nocc
{
struct WALBeforeAfterImage : WALEntry {
   u16 image_size;
   u8 payload[];
};
struct WALInitPage : WALEntry {
   DTID dt_id;
};
struct WALAfterImage : WALEntry {
   u16 image_size;
   u8 payload[];
};
struct WALLogicalSplit : WALEntry {
   PID parent_pid = -1;
   PID left_pid = -1;
   PID right_pid = -1;
   s32 right_pos = -1;
};
struct WALInsert : WALEntry {
   u16 key_length;
   u16 value_length;
   u8 payload[];
};
struct WALUpdate : WALEntry {
   u16 key_length;
   u8 payload[];
};
struct WALRemove : WALEntry {
   u16 key_length;
   u16 value_length;
   u8 payload[];
};
}  // namespace nocc
// -------------------------------------------------------------------------------------
enum class OP_TYPE : u8 { POINT_READ, POINT_UPDATE, POINT_INSERT, POINT_REMOVE, SCAN };
enum class OP_RESULT : u8 {
   OK = 0,
   NOT_FOUND = 1,
   DUPLICATE = 2,
   ABORT_TX = 3,
};
struct WALUpdateGenerator {
   void (*before)(u8* tuple, u8* entry);
   void (*after)(u8* tuple, u8* entry);
   u16 entry_size;
};
// -------------------------------------------------------------------------------------
struct BTree {
   // Interface
   /**
    * @brief Lookup one record via key. Read via payload_callback from record.
    * 
    * @param key Key to match
    * @param key_length Length of key
    * @param payload_callback Function to extract data from record.
    * @return OP_RESULT
    */
   OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
   /**
    * @brief Insert one record with key into storage.
    * 
    * @param key Key to insert
    * @param key_length Length of key
    * @param valueLength Length of record payload
    * @param value Record payload
    * @return OP_RESULT 
    */
   OP_RESULT insert(u8* key, u16 key_length, u64 valueLength, u8* value);
   /**
    * @brief Replace part record for key via callback function
    * 
    * @param key Key to match
    * @param key_length Length of key
    * @param callback Function to replace part of record.
    * @return OP_RESULT 
    */
  OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)> callback, WALUpdateGenerator = {{}, {}, 0});
   /**
    * @brief Deletes key from Storage
    * 
    * @param key Key to match
    * @param key_length Length of key
    * @return OP_RESULT 
    */
   OP_RESULT remove(u8* key, u16 key_length);
   /**
    * @brief Scans the storage from start_key in ascending order.
    * 
    * @param start_key Key where to start
    * @param key_length Length of key
    * @param callback Function to determine, if scan should be continued
    * @param undo What to do, if scan fails
    * @return OP_RESULT 
    */
   OP_RESULT scanAsc(u8* start_key, u16 key_length, function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback, function<void()> undo);
   /**
    * @brief 
    * 
    * @param start_key Scans the storage from start_key in descending order.
    * @param key_length length of the key
    * @param callback Function to determine, if scan should be continued
    * @param undo What to do, if scan fails
    * @return OP_RESULT 
    */
   OP_RESULT scanDesc(u8* start_key, u16 key_length, function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback, function<void()> undo);
// -------------------------------------------------------------------------------------
#include "BTreeLL.hpp"
// -------------------------------------------------------------------------------------
// SI
#include "BTreeSI.hpp"
// -------------------------------------------------------------------------------------
// VI [WIP]
#include "BTreeVI.hpp"
// -------------------------------------------------------------------------------------
// VW [WIP]
#include "BTreeVW.hpp"
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
