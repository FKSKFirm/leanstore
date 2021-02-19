#ifndef BTREE_LSMTREE_LSM_HPP
#define BTREE_LSMTREE_LSM_HPP

#include <cstdint>
#include <memory>
#include <vector>
#include "bloomFilter.hpp"
#include "btree.hpp"

using namespace leanstore::storage::btree;

struct StaticBTree {
   BTreeLL tree;
   uint64_t entryCount;
   BloomFilter filter;
};

struct LSM {
   uint64_t baseLimit = (1024*1024*8)/btreePageSize;
   uint64_t factor = 10;

   // pointer to inMemory BTree (Root of LSM-Tree)
   std::unique_ptr<BTreeLL> inMemBTree;
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
};

#endif  // BTREE_LSMTREE_LSM_HPP
