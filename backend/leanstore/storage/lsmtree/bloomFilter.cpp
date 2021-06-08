#include "bloomFilter.hpp"
#include <cmath>
#include <iostream>

using namespace leanstore::storage;

namespace leanstore
{
namespace storage
{
namespace lsmTree
{
unsigned intlog2(uint64_t x)
{
   return 64 - __builtin_clzl(x);
}

void BloomFilter::create(DTID dtid, DataStructureIdentifier* dsi, uint64_t n)
{
   // check if old bloom filter exists and has same count of pages
   // if yes:
   //    return, can insert in this bloomFilter
   // else:
   //    reclaim all pages (could also reuse them, more difficult)
   //    create new bloom filter with #needed pages

   if (!FLAGS_lsm_bloomFilter) {
      return;
   }

   uint64_t sizeBytes = (n * 4) / 8;
   uint64_t pagesBitsNew = 1 + (intlog2(sizeBytes / btree::btreePageSize));
   while (true) {
      jumpmuTry()
      {
         if (rootBloomFilterPage != nullptr) {
            // old bloom filter exists
            if (pageCount() == (1ull << pagesBitsNew)) {
               WorkerCounters::myCounters().lsm_reusedBloomFilters[dtid]++;
               jumpmu_return;
            } else {
               // we need a new bloom filter
               // reclaim old pages
               HybridPageGuard<BloomFilterPage> rootNode = HybridPageGuard<BloomFilterPage>(rootBloomFilterPage);
               deleteBloomFilterPages(rootNode);
            }
         }
         WorkerCounters::myCounters().lsm_createdBloomFilters[dtid]++;
         pagesBits = pagesBitsNew;

         // create new BloomFilter
         this->dt_id = dtid;
         this->type = dsi->type;
         this->level = dsi->level;

         // create root node
         auto root_write_guard_h = HybridPageGuard<BloomFilterPage>(dtid);
         auto root_write_guard = ExclusivePageGuard<BloomFilterPage>(std::move(root_write_guard_h));
         root_write_guard.bf()->header.keep_in_memory = true;
         root_write_guard.init();
         // set the dsi for the root node
         root_write_guard->type = dsi->type;
         root_write_guard->level = dsi->level;
         this->rootBloomFilterPage = root_write_guard.bf();

         // pageCount= 1, 2, 4, 8, 16, 32, ...
         if (pageCount() > 1) {
            // root node is not large enough, need of at least one additional level of the "Btree"
            root_write_guard->isLeaf = false;
            pageLevels = 2;
            // generate children of the root
            generateNewBloomFilterLevel(root_write_guard, pageCount());
         } else {
            // one page is enough
            root_write_guard->isLeaf = true;
            pageLevels = 1;
         }
         jumpmu_return;
      }
      jumpmuCatch() { }
   }
}

BloomFilter::BloomFilter() : pagesBits(0) {}

void BloomFilter::generateNewBloomFilterLevel(ExclusivePageGuard<BloomFilterPage>& page, uint64_t pagesToInsert)
{
   if (page->isLeaf) {
      return;
   } else {
      // pointers to the child pages doesnt fit in one root page, need of minimum one additional level
      uint64_t pageCountOnNextLevel = ceil(pagesToInsert / (double)BloomFilterPage::sizeOfFittingPtr);
      if (pageCountOnNextLevel > 1) {
         // further level required
         for (unsigned i = 0; i < BloomFilterPage::sizeOfFittingPtr; i++) {
            while (true) {
               jumpmuTry()
               {
                  // generate new non-leaf-level
                  auto new_node_h = HybridPageGuard<BloomFilterPage>(this->dt_id);
                  auto new_node = ExclusivePageGuard<BloomFilterPage>(std::move(new_node_h));
                  new_node.init();
                  new_node.bf()->header.keep_in_memory = true;

                  new_node->type = this->type;
                  new_node->level = this->level;
                  new_node->isLeaf = false;
                  page->pointerToChildren[i] = new_node.swip();

                  generateNewBloomFilterLevel(new_node, pageCountOnNextLevel);
                  jumpmu_break;
               }
               jumpmuCatch() { }
            }
         }
         page->count = BloomFilterPage::sizeOfFittingPtr;
         pageLevels++;
         return;
      } else {
         for (unsigned i = 0; i < pagesToInsert; i++) {
            while (true) {
               jumpmuTry()
               {
                  // leaf level
                  auto new_node_h = HybridPageGuard<BloomFilterPage>(this->dt_id);
                  auto new_node = ExclusivePageGuard<BloomFilterPage>(std::move(new_node_h));
                  new_node.init();
                  new_node.bf()->header.keep_in_memory = true;

                  new_node->type = this->type;
                  new_node->level = this->level;
                  new_node->isLeaf = true;
                  page->pointerToChildren[i] = new_node.swip();
                  jumpmu_break;
               }
               jumpmuCatch() { }
            }
         }
         page->count = pagesToInsert;
         pagesLowestLevel = pagesToInsert;
      }
   }
}

void BloomFilter::insert(uint64_t h)
{
   if (!FLAGS_lsm_bloomFilter) {
      return;
   }

   // pagesBits are the highest n Bits, identifying the page with wordMask x uint64_t
   unsigned pageNumber = h >> (64 - pagesBits);
   HybridPageGuard<BloomFilterPage> currentPage = HybridPageGuard<BloomFilterPage>(rootBloomFilterPage);

   unsigned currentPageLevel = pageLevels;
   while (!currentPage->isLeaf) {
      uint64_t numberOfEntriesBelowNodePerChild =
          1;  // if root node has between 2...512 children, these children are payload nodes, so per child one entry page
      if (currentPageLevel > 2) {
         // node is a inner node with more than 512 children, therefore has 512 links to child nodes
         numberOfEntriesBelowNodePerChild = pagesLowestLevel;
      }

      for (unsigned i = currentPageLevel; i > 3; i--) {
         // for each extra level of 512 child pointers
         numberOfEntriesBelowNodePerChild *= BloomFilterPage::sizeOfFittingPtr;
      }

      unsigned positionOfNextChildNode = 0;
      while (pageNumber >= (positionOfNextChildNode + 1) * numberOfEntriesBelowNodePerChild) {
         positionOfNextChildNode++;
      }

      Swip<BloomFilterPage>& childToFollow = currentPage->pointerToChildren[positionOfNextChildNode];
      currentPage = HybridPageGuard<BloomFilterPage>(currentPage, childToFollow);

      pageNumber -= positionOfNextChildNode * numberOfEntriesBelowNodePerChild;
      currentPageLevel--;
      assert(positionOfNextChildNode < (btree::btreePageSize / sizeof(BloomFilterPage*)));
   }
   ExclusivePageGuard<BloomFilterPage> p = ExclusivePageGuard<BloomFilterPage>(std::move(currentPage));

   //BloomFilterPage& p = pages[pageNumber];
   uint64_t searchMask = 0;

   // 1ull = 64 Bits = 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0001
   // 63 = 0011 1111
   // set/shift 5x one bit between position 0 and 63 in searchMask
   // identified by the last 6 Bit of h
   searchMask |= 1ull << ((h >> 0) & 63);
   // with Bits 7-12 of h
   searchMask |= 1ull << ((h >> 6) & 63);
   // with Bits 13-18 of h
   searchMask |= 1ull << ((h >> 12) & 63);
   // with Bits 19-24 of h
   searchMask |= 1ull << ((h >> 18) & 63);
   // with Bits 25-30 of h
   searchMask |= 1ull << ((h >> 24) & 63);

   // wordMask=511 = 0001 1111 1111 for btreePageSize=4096
   // select uint64_t word based on bits 31-39 (depending on wordMask) of h
   assert(((h >> 30) & BloomFilterPage::wordMask) < (btree::btreePageSize / sizeof(uint64_t)));
   uint64_t& word = p->word[(h >> 30) & BloomFilterPage::wordMask];
   word |= searchMask;
   // std::cout << "Insert in pageNumber: " << pageNumber << std::endl;
   // std::cout << "Insert in currentPage: " << currentPage << std::endl;
}

void BloomFilter::insert(uint8_t* key, unsigned len)
{
   insert(hashKey(key, len));
}

bool BloomFilter::lookup(uint8_t* key, unsigned len)
{
   if (!pagesBits)
      return true;
   uint64_t h = hashKey(key, len);
   unsigned pageNumber = h >> (64 - pagesBits);
   HybridPageGuard<BloomFilterPage> currentPage = HybridPageGuard<BloomFilterPage>(rootBloomFilterPage);
   unsigned currentPageLevel = pageLevels;
   while (!currentPage->isLeaf) {
      uint64_t numberOfEntriesBelowNodePerChild =
          1;  // if root node has between 2...512 children, these children are payload nodes, so per child one entry page
      if (currentPageLevel > 2) {
         // node is a inner node with more than 512 children, therefore has 512 links to child nodes
         numberOfEntriesBelowNodePerChild = pagesLowestLevel;
      }

      for (unsigned i = currentPageLevel; i > 3; i--) {
         // for each extra level of 512 child pointers
         numberOfEntriesBelowNodePerChild *= BloomFilterPage::sizeOfFittingPtr;
      }

      int positionOfNextChildNode = 0;
      while (pageNumber >= (positionOfNextChildNode + 1) * numberOfEntriesBelowNodePerChild) {
         positionOfNextChildNode++;
      }

      Swip<BloomFilterPage>& childToFollow = currentPage->pointerToChildren[positionOfNextChildNode];
      currentPage = HybridPageGuard<BloomFilterPage>(currentPage, childToFollow);

      pageNumber -= positionOfNextChildNode * numberOfEntriesBelowNodePerChild;
      currentPageLevel--;
   }

   uint64_t searchMask = 0;
   searchMask |= 1ull << ((h >> 0) & 63);
   searchMask |= 1ull << ((h >> 6) & 63);
   searchMask |= 1ull << ((h >> 12) & 63);
   searchMask |= 1ull << ((h >> 18) & 63);
   searchMask |= 1ull << ((h >> 24) & 63);

   uint64_t word = currentPage->word[(h >> 30) & BloomFilterPage::wordMask];
   return ((word & searchMask) == searchMask);
}

void BloomFilter::deleteBloomFilterPages(HybridPageGuard<BloomFilterPage>& node) {
   // we need no jumpmutry because all pages are kept in memory
   if (!FLAGS_lsm_bloomFilter) {
      return;
   }

   ExclusivePageGuard<BloomFilterPage> nodeX = ExclusivePageGuard<BloomFilterPage>(std::move(node));
   if (node->isLeaf) {
      nodeX.reclaim();
   }
   else {
      // reclaim children first
      for (unsigned i = 0; i < node->count; i++) {
         Swip<BloomFilterPage>& childToFollow = node->pointerToChildren[i];
         HybridPageGuard<BloomFilterPage> child = HybridPageGuard<BloomFilterPage>(node, childToFollow);
         deleteBloomFilterPages(child);
      }
      nodeX.reclaim();
   }
}

}
}
}
