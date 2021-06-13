#ifndef BTREE_LSMTREE_LSM_HPP
#define BTREE_LSMTREE_LSM_HPP

#include <cstdint>
#include <memory>
#include <vector>
#include "bloomFilter.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

using namespace leanstore::storage;

namespace leanstore
{
namespace storage
{
namespace lsmTree
{
struct StaticBTree {
   std::unique_ptr<btree::BTreeLL> tree;
   std::unique_ptr<BloomFilter> filter;
   StaticBTree() {
      tree = make_unique<btree::BTreeLL>();
      filter = make_unique<BloomFilter>();
   }
   ~StaticBTree(){}
};

struct LSM : public KVInterface {
   //half buffer for inMemBTree
   uint64_t baseLimit = ((1024 * 1024 * 1024 * FLAGS_dram_gib) / PAGE_SIZE) * FLAGS_lsm_inMemBTreeSize;
   uint64_t factor = FLAGS_lsm_tieringFactor;

   // pointer to inMemory BTree (Root of LSM-Tree)
   std::unique_ptr<btree::BTreeLL> inMemBTree;
   std::vector<std::unique_ptr<StaticBTree>> tiers;
   DTID dt_id;
   BufferFrame* meta_node_bf;  // kept in memory

   // 0 = inMemBTree with tiers[0], 1 = tiers[0] with tiers[1], ...
   unsigned levelInMerge;
   // if levelInMerge == 0, a new inMemBTree is created and the oldInMemBTree is moved to inMemBTreeInMerge
   std::unique_ptr<btree::BTreeLL> inMemBTreeInMerge;
   std::mutex merge_mutex;

   LSM();
   ~LSM();

   void create(DTID dtid, BufferFrame* meta_bf);

   void printLevels();

   // merges
   void mergeAll();
   unique_ptr<StaticBTree> mergeTrees(unique_ptr<StaticBTree>& levelToReplace, btree::BTreeLL* aTree, btree::BTreeLL* bTree);

   // returns true when the key is found in the inMemory BTree (Root of LSM-Tree)
   //bool lookup(uint8_t* key, unsigned keyLength);
   //bool lookupOnlyBloomFilter(uint8_t* key, unsigned int keyLength);

   OP_RESULT updateSameSizeInPlace(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>) override;
   OP_RESULT remove(u8* key, u16 key_length) override;
   OP_RESULT scanAsc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) override;
   OP_RESULT scanDesc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) override;
   u64 countPages() override;
   u64 countEntries() override;
   u64 getHeight() override;
   OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   OP_RESULT insertWithDeletionMarker(u8* key, u16 key_length, u8* value, u16 value_length, bool deletionMarker);
   OP_RESULT insertWithDeletionMarkerUpdateMarker(u8* key, u16 keyLength, u8* payload, u16 payloadLength, bool deletionMarker, bool updateMarker);
   OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;

   static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   static DTRegistry::DTMeta getMeta();
   static void iterateChildrenSwips(void* btree_object, BufferFrame& bufferFrame, std::function<bool(Swip<BufferFrame>&)> callback);
   static bool checkSpaceUtilization(void* btree_object, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
};
}
}
}

#endif  // BTREE_LSMTREE_LSM_HPP
