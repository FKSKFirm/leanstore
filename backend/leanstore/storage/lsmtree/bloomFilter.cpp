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

BloomFilter::BloomFilter() : pagesBits(0) {}

void BloomFilter::generateNewBloomFilterLevel(BloomFilter::Page* page, uint64_t pagesToInsert)
{
   if (page->isLeaf) {
      return;
   } else {
      // pointers to the child pages doesnt fit in one root page, need of minimum one additional level
      uint64_t pageCountOnNextLevel = ceil(pagesToInsert / (double)sizeOfFittingPtr);
      if (pageCountOnNextLevel > 1) {
         // further level required
         for (int i = 0; i < sizeOfFittingPtr; i++) {
            // generate new non-leaf-level
            Page* newPage = new Page();
            newPage->isLeaf = false;
            page->pointerToChilds[i] = newPage;
            generateNewBloomFilterLevel(page->pointerToChilds[i], pageCountOnNextLevel);
         }
         pageLevels++;
         return;
      } else {
         for (int i = 0; i < pagesToInsert; i++) {
            // leaf level
            Page* newPage = new Page();
            newPage->isLeaf = true;
            page->pointerToChilds[i] = newPage;
         }
         pagesLowestLevel = pagesToInsert;
      }
   }
}

void BloomFilter::init(uint64_t n)
{
   uint64_t sizeBytes = (n * 4) / 8;
   pagesBits = 1 + (intlog2(sizeBytes / btree::btreePageSize));
   pageCount = 1ull << pagesBits;

   //std::cout << "New BloomFilter with #pages: " << pageCount << std::endl;
   rootBloomFilterPage = new Page();

   // pageCount= 1, 2, 4, 8, 16, 32, ...
   if (pageCount > 1) {
      // root node is not large enough, need of at least one additional level of the "Btree"
      rootBloomFilterPage->isLeaf = false;
      pageLevels = 2;
      // generate Childs of the root
      generateNewBloomFilterLevel(rootBloomFilterPage, pageCount);
   } else {
      // one page is enough
      rootBloomFilterPage->isLeaf = true;
      pageLevels = 1;
   }

   pages.resize(1ull << pagesBits);
}

void BloomFilter::insert(uint64_t h)
{
   // pagesBits are the highest n Bits, identifying the page with wordMask x uint64_t
   int pageNumber = h >> (64 - pagesBits);
   Page* currentPage = rootBloomFilterPage;
   int currentPageLevel = pageLevels;
   while (!currentPage->isLeaf) {
      uint64_t numberOfEntriesBelowNodePerChild =
          1;  // if root node has between 2...512 children, these children are payload nodes, so per child one entry page
      if (currentPageLevel > 2) {
         // node is a inner node with more than 512 children, therefore has 512 links to child nodes
         numberOfEntriesBelowNodePerChild = pagesLowestLevel;
      }

      for (int i = currentPageLevel; i > 3; i--) {
         // for each extra level of 512 child pointers
         numberOfEntriesBelowNodePerChild *= sizeOfFittingPtr;
      }

      int positionOfNextChildNode = 0;
      while (pageNumber >= (positionOfNextChildNode + 1) * numberOfEntriesBelowNodePerChild) {
         positionOfNextChildNode++;
      }

      pageNumber -= positionOfNextChildNode * numberOfEntriesBelowNodePerChild;
      currentPageLevel--;
      assert(positionOfNextChildNode >= 0);
      assert(positionOfNextChildNode < (btree::btreePageSize / sizeof(Page*)));
      currentPage = currentPage->pointerToChilds[positionOfNextChildNode];
   }

   Page& p = pages[pageNumber];
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
   uint64_t& word = p.word[(h >> 30) & wordMask];
   uint64_t& word2 = currentPage->word[(h >> 30) & wordMask];
   word |= searchMask;
   word2 |= searchMask;
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
      return false;
   uint64_t h = hashKey(key, len);
   /*
         int pageNumber = h>>(64-pagesBits);
         Page *currentPage = rootBloomFilterPage;
         int currentPageLevel = pageLevels;
         while (!currentPage->isLeaf) {
            uint64_t numberOfEntriesBelowNode = 1;
            for (int i=currentPageLevel; i>1; i--) {
               numberOfEntriesBelowNode *= sizeOfFittingPtr;
            }
            uint64_t numbersOfEntriesPerNode = numberOfEntriesBelowNode/sizeOfFittingPtr;

            int positionOfNextChildNode = 1;
            while (pageNumber >= positionOfNextChildNode * numbersOfEntriesPerNode) {
               positionOfNextChildNode++;
            }

            pageNumber -= (positionOfNextChildNode-1) * numbersOfEntriesPerNode;
            currentPageLevel--;
            currentPage = currentPage->pointerToChilds[positionOfNextChildNode-1];
         }
   */
   int pageNumber = h >> (64 - pagesBits);
   Page* currentPage = rootBloomFilterPage;
   int currentPageLevel = pageLevels;
   while (!currentPage->isLeaf) {
      uint64_t numberOfEntriesBelowNodePerChild =
          1;  // if root node has between 2...512 children, these children are payload nodes, so per child one entry page
      if (currentPageLevel > 2) {
         // node is a inner node with more than 512 children, therefore has 512 links to child nodes
         numberOfEntriesBelowNodePerChild = pagesLowestLevel;
      }

      for (int i = currentPageLevel; i > 3; i--) {
         // for each extra level of 512 child pointers
         numberOfEntriesBelowNodePerChild *= sizeOfFittingPtr;
      }

      int positionOfNextChildNode = 0;
      while (pageNumber >= (positionOfNextChildNode + 1) * numberOfEntriesBelowNodePerChild) {
         positionOfNextChildNode++;
      }

      pageNumber -= positionOfNextChildNode * numberOfEntriesBelowNodePerChild;
      currentPageLevel--;
      currentPage = currentPage->pointerToChilds[positionOfNextChildNode];
   }

   Page& p = pages[pageNumber];

   uint64_t searchMask = 0;
   searchMask |= 1ull << ((h >> 0) & 63);
   searchMask |= 1ull << ((h >> 6) & 63);
   searchMask |= 1ull << ((h >> 12) & 63);
   searchMask |= 1ull << ((h >> 18) & 63);
   searchMask |= 1ull << ((h >> 24) & 63);

   uint64_t word = currentPage->word[(h >> 30) & wordMask];
   return ((word & searchMask) == searchMask);
   // old:
   // uint64_t word = p.word[(h>>30) & wordMask];
   // return ((word & searchMask) == searchMask);
}
}
}
}
