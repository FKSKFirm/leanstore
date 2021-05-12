#include "BTreeLL.hpp"

#include "core/BTreeGenericIterator.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
// lookup for a value, identified by key, key has length of key_length; payload_callback checks if it is the correct and expected payload
OP_RESULT BTreeLL::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         /*DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key, key_length);
            leaf.recheck();
            if (sanity_check_result != 0) {
               cout << leaf->count << endl;
            }
            ensure(sanity_check_result == 0);
         }*/
         // -------------------------------------------------------------------------------------
         s16 pos = leaf->lowerBound<true>(key, key_length);
         if (pos != -1) {
            //check if the LSM-deletion flag is set
            if (leaf->isDeleted(pos))
               return OP_RESULT::LSM_DELETED;

            // deletion flag is not set, normal lookup
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck();
            jumpmu_return OP_RESULT::OK;
         } else {
            leaf.recheck();
            // dont raise SIGTRAP, because if one value does not occur it may be in an other LSM Tree layer
            //raise(SIGTRAP);
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback)
{
   Slice key(start_key, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seek(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         } else {
            if (iterator.next() != OP_RESULT::OK) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback)
{
   Slice key(start_key, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekForPrev(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         } else {
            if (iterator.prev() != OP_RESULT::OK) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::insert(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length)
{
   return insertWithDeletionMarker(o_key, o_key_length, o_value, o_value_length, false);
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::insertWithDeletionMarker(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length, bool deletionMarker)
{
   Slice key(o_key, o_key_length);
   Slice value(o_value, o_value_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.insertKVWithDeletionMarker(key, value, deletionMarker);
      ensure(ret == OP_RESULT::OK);
      iterator.leaf.incrementGSN();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::updateSameSize(u8* o_key,
                                  u16 o_key_length,
                                  function<void(u8* payload, u16 payload_size)> callback)
{
   Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      auto current_value = iterator.mutableValue();
      callback(current_value.data(), current_value.length());
      iterator.leaf.incrementGSN();
      iterator.contentionSplit();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::remove(u8* o_key, u16 o_key_length)
{
   Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      iterator.leaf.incrementGSN();
      ret = iterator.removeCurrent();
      ensure(ret == OP_RESULT::OK);
      iterator.mergeIfNeeded();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::removeWithDeletionMarker(u8* o_key, u16 o_key_length)
{
   Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      iterator.leaf.incrementGSN();
      iterator.leaf->setDeletedFlag(iterator.getSlot());
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::countEntries()
{
   return BTreeGeneric::countEntries();
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::countPages()
{
   return BTreeGeneric::countPages();
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::getHeight()
{
   return BTreeGeneric::getHeight();
}
// -------------------------------------------------------------------------------------
struct DataTypeRegistry::DTMeta BTreeLL::getMeta()
{
   DataTypeRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
                                    .find_parent = findParent,
                                    .check_space_utilization = checkSpaceUtilization};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
struct ParentSwipHandler BTreeLL::findParent(void* btree_object, BufferFrame& to_find)
{
   return BTreeGeneric::findParent(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), to_find);
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
