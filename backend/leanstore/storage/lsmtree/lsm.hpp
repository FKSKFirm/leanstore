#ifndef BTREE_LSMTREE_LSM_HPP
#define BTREE_LSMTREE_LSM_HPP

#include <cstdint>
#include <memory>
#include <vector>
#include "bloomFilter.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/KeyValueInterface.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"

using namespace leanstore::storage;

namespace leanstore
{
namespace storage
{
namespace lsmTree
{
struct StaticBTree {
   btree::BTreeLL tree;
   uint64_t entryCount;
   BloomFilter filter;
};

struct LSM : public KeyValueInterface {
   uint64_t baseLimit = (1024 * 1024 * 8) / btree::btreePageSize;
   uint64_t factor = 10;

   // pointer to inMemory BTree (Root of LSM-Tree)
   std::unique_ptr<btree::BTreeLL> inMemBTree;
   std::vector<std::unique_ptr<StaticBTree>> tiers;

   LSM();
   ~LSM();

   void printLevels();

   // merges
   void mergeAll();

   void insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   // returns true when the key is found in the inMemory BTree (Root of LSM-Tree)
   bool lookup(uint8_t* key, unsigned keyLength);
   bool lookupOnlyBloomFilter(uint8_t* key, unsigned int keyLength);



   virtual OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>) override;
   virtual OP_RESULT remove(u8* key, u16 key_length) override;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) override;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) override;
   virtual u64 countPages() override;
   virtual u64 countEntries() override;
   virtual u64 getHeight() override;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   virtual OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;

   static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   static DataTypeRegistry::DTMeta getMeta();
   static void iterateChildrenSwips(void* btree_object, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback);
   static bool checkSpaceUtilization(void* btree_object, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);

};
}
}
}

#endif  // BTREE_LSMTREE_LSM_HPP
