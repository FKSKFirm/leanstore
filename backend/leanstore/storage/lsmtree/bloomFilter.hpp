#ifndef BTREE_LSMTREE_BLOOMFILTER_HPP
#define BTREE_LSMTREE_BLOOMFILTER_HPP

#include <cstdint>
#include <cstring>
#include <leanstore/storage/DataStructureIdentifier.hpp>
#include <vector>
#include "MurmurHash3.h"

#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
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
// BloomFilter for performance lookups in BTrees, LSM-Trees...
// Fixed size from the beginning, can not resize
// Therefore fixed Tree structure
struct BloomFilter : public DataStructureIdentifier {
   static uint64_t hashKey(uint8_t* key, unsigned len)
   {
      uint64_t out[2];
      MurmurHash3_x64_128(key, len, 137, &out);
      return out[1];
   }

   struct BloomFilterPage : public DataStructureIdentifier {
      bool isLeaf;
      unsigned count;
      static const uint64_t sizeOfFittingPtr = ((EFFECTIVE_PAGE_SIZE - sizeof(DataStructureIdentifier) - sizeof(count) - sizeof(isLeaf)) / sizeof(Swip<BloomFilterPage>*));
      static const uint64_t wordMask = ((EFFECTIVE_PAGE_SIZE - sizeof(DataStructureIdentifier) - sizeof(count) - sizeof(isLeaf)) / sizeof(uint64_t)) - 1;
      union {
         uint64_t word[((EFFECTIVE_PAGE_SIZE - sizeof(DataStructureIdentifier) - sizeof(count) - sizeof(isLeaf)) / sizeof(uint64_t))];
         Swip<BloomFilterPage> pointerToChildren[sizeOfFittingPtr];
      };
      BloomFilterPage() { memset(word, 0, sizeof(word)); };
   };

   DTID dt_id;
   uint64_t pagesBits;
   uint64_t pageLevels = 0;
   uint64_t pagesLowestLevel = 0;
   BufferFrame* rootBloomFilterPage = nullptr;

   BloomFilter();
   ~BloomFilter() {}

   uint64_t pageCount() {return (1ull << pagesBits);}

   void create(DTID dtid, DataStructureIdentifier* dsi, uint64_t n);
   void insert(uint64_t h);
   void insert(uint8_t* key, unsigned len);
   bool lookup(uint8_t* key, unsigned len);
   void generateNewBloomFilterLevel(ExclusivePageGuard<BloomFilterPage>& page, uint64_t pagesToInsert);
   void deleteBloomFilterPages(HybridPageGuard<BloomFilterPage>& node);
};
}
}
}

#endif  // BTREE_LSMTREE_BLOOMFILTER_HPP
