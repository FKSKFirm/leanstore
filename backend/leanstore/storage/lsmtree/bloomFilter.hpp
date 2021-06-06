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
//const uint64_t btreePageSize = sizeof(BufferFrame::Page::dt);

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
   // TODO: set correct PageSize: (static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(BufferFrame::Page::dt);)
   // TODO: Tree-structure for LeanStore
   struct Page {
      bool isLeaf;
      union {
         uint64_t word[(btree::btreePageSize / sizeof(uint64_t))];
         Page* pointerToChilds[(btree::btreePageSize / sizeof(Page*))];
      };
      Page() { memset(word, 0, sizeof(word)); };
   };

   uint64_t pagesBits;
   uint64_t pageCount = 0;
   uint64_t pagesLowestLevel = 0;
   Page* rootBloomFilterPage;
   uint64_t pageLevels = 0;
   std::vector<Page> pages;
   static const uint64_t wordMask = (btree::btreePageSize / sizeof(uint64_t)) - 1;
   static const uint64_t sizeOfFittingPtr = sizeof(rootBloomFilterPage->pointerToChilds) / sizeof(Page*);

   BloomFilter();
   ~BloomFilter() { pages.clear(); };

   void init(uint64_t n);

   void insert(uint64_t h);

   void insert(uint8_t* key, unsigned len);

   bool lookup(uint8_t* key, unsigned len);
   void generateNewBloomFilterLevel(Page* page, uint64_t pagesToInsert);
};
}
}
}

#endif  // BTREE_LSMTREE_BLOOMFILTER_HPP
