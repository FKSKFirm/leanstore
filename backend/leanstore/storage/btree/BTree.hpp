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
// -------------------------------------------------------------------------------------
enum class OP_TYPE : u8 { POINT_READ, POINT_UPDATE, POINT_INSERT, POINT_REMOVE, SCAN };
enum class OP_RESULT : u8 {
   OK = 0,
   NOT_FOUND = 1,
   DUPLICATE = 2,
   ABORT_TX = 3,
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
  OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)> callback);
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
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
