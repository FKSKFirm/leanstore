#ifndef BTREE_LSMTREE_BLOOMFILTER_HPP
#define BTREE_LSMTREE_BLOOMFILTER_HPP

#include <cstdint>
#include <cstring>
#include <vector>
#include "MurmurHash3.h"
#include "btree.hpp"

using namespace leanstore::storage::btree;

const uint64_t btreePageSize = sizeof(BufferFrame::Page::dt);

// BloomFilter for performance lookups in BTrees, LSM-Trees...
// Fixed size from the beginning, can not resize
// Therefore fixed Tree structure
struct BloomFilter {
   static uint64_t hashKey(uint8_t* key, unsigned len) {
      uint64_t out[2];
      MurmurHash3_x64_128(key, len, 137, &out);
      return out[1];
   }
   // TODO: set correct PageSize: (static constexpr u64 EFFECTIVE_PAGE_SIZE = sizeof(BufferFrame::Page::dt);)
   // TODO: Tree-structure for LeanStore
   struct Page {
      Page(Page* pPage) { memset(word, 0, sizeof(word)); }
      bool isLeaf;
      union {
         uint64_t word[(btreePageSize /sizeof(uint64_t))];
         Page* pointerToChilds[(btreePageSize /sizeof(Page*))];
      };
      Page() { memset(word, 0, sizeof(word)); };
   };

   uint64_t pagesBits;
   uint64_t pageCount=0;
   uint64_t pagesLowestLevel=0;
   Page *rootBloomFilterPage;
   uint64_t pageLevels=0;
   std::vector<Page> pages;
   static const uint64_t wordMask = (btreePageSize /sizeof(uint64_t))-1;
   static const uint64_t sizeOfFittingPtr = sizeof(rootBloomFilterPage->pointerToChilds) / sizeof(Page*);

   BloomFilter();

   void init(uint64_t n);

   void insert(uint64_t h);

   void insert(uint8_t* key, unsigned len);

   bool lookup(uint8_t* key, unsigned len);
   void generateNewBloomFilterLevel(Page* page, uint64_t pagesToInsert);
};

#endif  // BTREE_LSMTREE_BLOOMFILTER_HPP
