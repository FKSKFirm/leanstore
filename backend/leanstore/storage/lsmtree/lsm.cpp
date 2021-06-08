#include "lsm.hpp"
#include <cstdint>
#include <iostream>
#include <leanstore/storage/btree/core/BTreeGenericIterator.hpp>
#include <leanstore/storage/btree/core/BTreeNode.hpp>
#include <leanstore/storage/lsmtree/LSMBTreeMerger.hpp>

using namespace std;
using namespace leanstore::storage;

namespace leanstore
{
namespace storage
{
namespace lsmTree
{
struct KeyValueEntry {
   unsigned keyOffset;
   unsigned keyLen;
   unsigned payloadOffset;
   unsigned payloadLen;
   bool deletionFlag;
};

// Compare two string
// returns a negative value when a is smaller than b
// returns 0 when both values are the same
// returns a value > 0 when b is the smaller value
static int cmpKeys(uint8_t* a, uint8_t* b, unsigned aLength, unsigned bLength)
{
   int c = memcmp(a, b, min(aLength, bLength));
   if (c)
      return c;
   return (aLength - bLength);
}

static int cmpKeysString(Slice a, Slice b, unsigned aLength, unsigned bLength)
{
   int c = memcmp(a.data(), b.data(), min(aLength, bLength));
   if (c)
      return c;
   return (aLength - bLength);
}

unsigned spaceNeeded(unsigned prefix, unsigned totalKeySize, unsigned totalPayloadSize, unsigned count, unsigned fencesLength)
{
   // spaceNeeded = BTreeNodeHeader + slots * count + totalKeyAndPayloadSize + lowerFence + upperFence
   return sizeof(btree::BTreeNodeHeader) + (sizeof(btree::BTreeNode::Slot) * count) + totalKeySize - (count * prefix) + fencesLength + totalPayloadSize;
}

// returns the number of same letters
unsigned commonPrefix(uint8_t* a, unsigned lenA, uint8_t* b, unsigned lenB)
{
   unsigned limit = min(lenA, lenB);
   unsigned i;
   for (i = 0; i < limit; i++)
      if (a[i] != b[i])
         break;
   return i;
}

void buildNode(JMUW<vector<KeyValueEntry>>& entries, JMUW<vector<uint8_t>>& keyStorage, JMUW<vector<uint8_t>>& payloadStorage, btree::BTreeLL& btree, bool isFirstNode, bool isLastNode)
{
   //added while true with jumpmuTry since new Page allocation failed (dram_free_list empty, partition.dram_free_list.pop() fails)
   while (true) {
      jumpmuTry()
      {
         auto new_node_h = HybridPageGuard<btree::BTreeNode>(btree.dt_id);
         auto new_node = ExclusivePageGuard<btree::BTreeNode>(std::move(new_node_h));
         new_node.init(true);

         new_node->type = LSM_TYPE::BTree;
         new_node->level = btree.level;
         new_node.bf()->header.keep_in_memory = false;

         //prefix in this node (key with "http://..." to "https://..." would have the prefix "http" with length 4
         new_node->prefix_length = commonPrefix(entries->back().keyOffset + keyStorage->data(), entries->back().keyLen,
                                           entries->front().keyOffset + keyStorage->data(), entries->front().keyLen);
         if (isFirstNode || isLastNode) {
            // first node must have a prefix length of 0, because the lower bound is zero (and the prefix is stored in the lower bound)
            new_node->prefix_length = 0;
         }
         new_node->count = entries->size()-1;
         assert(new_node->count>0);
         for (unsigned i = 1; i < entries->size(); i++) {
            //if (entries.obj[i].keyLen == 4)
               //cout << "Key: " << (__builtin_bswap32(*reinterpret_cast<const uint32_t*>(entries.obj[i].keyOffset + keyStorage->data())) ^ (1ul << 31)) << endl;
            //cout << "KeyLen: " << entries.obj[i].keyLen << endl;
            new_node->storeKeyValueWithDeletionMarker(i - 1, entries.obj[i].keyOffset + keyStorage->data(), entries.obj[i].keyLen,
                                                      entries.obj[i].payloadOffset + payloadStorage->data(), entries.obj[i].payloadLen,
                                                      entries.obj[i].deletionFlag);
         }
         if (!isFirstNode) {
            new_node->insertFence(new_node->lower_fence, entries->front().keyOffset + keyStorage->data(),
                                  entries->front().keyLen);
            //if (entries->front().keyLen == 4)
               //cout << "LowerFence: " << (__builtin_bswap32(*reinterpret_cast<const uint32_t*>(new_node->getLowerFenceKey())) ^ (1ul << 31)) << endl;
         }
         if (!isLastNode) {
            new_node->insertFence(new_node->upper_fence, entries->back().keyOffset + keyStorage->data(), entries->back().keyLen);
            //if (entries->back().keyLen == 4)
               //cout << "UpperFence: " << (__builtin_bswap32(*reinterpret_cast<const uint32_t*>(new_node->getUpperFenceKey())) ^ (1ul << 31)) << endl;
         }
         new_node->makeHint();

         while (true) {
            jumpmuTry()
            {
               FLAGS_bulk_insert = true;
               btree.insertLeafNode(entries->back().keyOffset + keyStorage->data(), entries->back().keyLen, new_node, isLastNode);
               FLAGS_bulk_insert = false;
               jumpmu_break;
            }
            jumpmuCatch() { cout << "InsertLeafNodeRetry" << endl; }
         }
         //cout << "Node Buffer: " << new_node.bf() << endl;

         jumpmu_break;
      }
      jumpmuCatch() { }
   }
}

// adds the prefix and the key to keyStorage and returns the length of prefix and key
unsigned bufferKey(LSMBTreeMerger& a, vector<uint8_t>& keyStorage)
{
   // insert the Prefix at the end of keyStorage
   unsigned pl = a.leaf->prefix_length;
   keyStorage.insert(keyStorage.end(), a.leaf->getLowerFenceKey(), a.leaf->getLowerFenceKey() + pl);

   // insert the key in the keyStorage
   unsigned kl = a.getKeyLen();
   keyStorage.insert(keyStorage.end(), a.getKey(), a.getKey() + kl);

   return pl + kl;
}

// adds the payload to payloadStorage and returns the length of the payload
unsigned bufferPayload(LSMBTreeMerger& a, vector<uint8_t>& payloadStorage)
{
   // insert the key in the keyStorage
   unsigned pll = a.getPayloadLen();
   payloadStorage.insert(payloadStorage.end(), a.getPayload(), a.getPayload() + pll);

   return pll;
}

/*
//Merge with usage of bulkInsert & insertion on entry-level (broken, because PageProvider does not find the old tree (not set as upper))
unique_ptr<StaticBTree> LSM::mergeTrees2(unique_ptr<StaticBTree>& levelToReplace, btree::BTreeLL* aTree, btree::BTreeLL* bTree)
{
   FLAGS_bulk_insert = true;
   auto newTree = make_unique<StaticBTree>();

   // a new merged tree is always on disk, never a In-memory tree
   newTree->tree.type = LSM_TYPE::BTree;
   newTree->filter.type = LSM_TYPE::BloomFilter;

   //dsi needed for root bTreeNode
   DataStructureIdentifier dsi = DataStructureIdentifier();
   dsi.type = LSM_TYPE::BTree;

   if (bTree == NULL && aTree->type == LSM_TYPE::InMemoryBTree) { // we create the first level on disk, so the new merged tree has level 0 on disk
      newTree->tree.level = 0;
      newTree->filter.level = 0;
      dsi.level = 0;
   }
   else if (bTree == NULL && aTree->type==LSM_TYPE::BTree) {
      newTree->tree.level = aTree->level + 1;
      newTree->filter.level = aTree->level + 1;
      dsi.level = aTree->level + 1;
   }
   else { // aTree is to large for level i, so its merged into bTree which resides at level i+1
      newTree->tree.level = bTree->level;
      newTree->filter.level = bTree->level;
      dsi.level = bTree->level;
   }

   while (true) {
      jumpmuTry()
         {
            // create new meta_node (could even pick the meta_node of aTree or bTree, but here we only know the root of these two trees, not the meta node)
            auto& newMetaNode = BMC::global_bf->allocatePage();
            Guard guard(newMetaNode.header.latch, GUARD_STATE::EXCLUSIVE);
            newMetaNode.header.keep_in_memory = true;
            newMetaNode.page.dt_id = this->dt_id;
            ((btree::BTreeNode*)newMetaNode.page.dt)->type = dsi.type;
            ((btree::BTreeNode*)newMetaNode.page.dt)->level = dsi.level;
            guard.unlock();

            newTree->tree.create(this->dt_id, &newMetaNode, &dsi);
            jumpmu_break;
         }
      jumpmuCatch() {}
   }

   merge_mutex.lock();
   bTreeInMerge = std::move(levelToReplace);
   levelToReplace = std::move(newTree);
   merge_mutex.unlock();

   LSMBTreeMerger aTreeLSMIterator(*aTree);
   LSMBTreeMerger bTreeLSMIterator(*bTree);
   LSMBTreeMerger* iterator;

   aTreeLSMIterator.moveToFirstLeaf();
   if(bTree==NULL) {
      bTreeLSMIterator.done = true;
   }
   else {
      bTreeLSMIterator.moveToFirstLeaf();
   }

   JMUW<vector<uint8_t>> keyA, keyB;
   keyA->reserve(btree::btreePageSize);
   keyB->reserve(btree::btreePageSize);

   uint64_t entryCount = 0;
   JMUW<vector<uint64_t>> hashes;

   jumpmuTry() {
      while (true) {
         if (aTreeLSMIterator.done) {
            if (bTreeLSMIterator.done) {
               // create BloomFilter
               levelToReplace->filter.init(entryCount);
               for (auto h : hashes.obj)
                  levelToReplace->filter.insert(h);

               // merge done
               jumpmuTry()
               {
                  // Reclaim the remaining meta & root nodes
                  HybridPageGuard<btree::BTreeNode> aTreeMetaNode = HybridPageGuard<btree::BTreeNode>(aTree->meta_node_bf);
                  HybridPageGuard<btree::BTreeNode> aTreeRootNode(aTreeMetaNode, aTreeMetaNode->upper);
                  aTreeRootNode.toExclusive();
                  aTreeMetaNode.toExclusive();
                  aTreeRootNode.reclaim();
                  aTreeMetaNode.reclaim();
                  if (bTree != NULL) {
                     HybridPageGuard<btree::BTreeNode> bTreeMetaNode = HybridPageGuard<btree::BTreeNode>(bTree->meta_node_bf);
                     HybridPageGuard<btree::BTreeNode> bTreeRootNode(bTreeMetaNode, bTreeMetaNode->upper);
                     bTreeRootNode.toExclusive();
                     bTreeMetaNode.toExclusive();
                     bTreeRootNode.reclaim();
                     bTreeMetaNode.reclaim();
                  }
               }
               jumpmuCatch() {}

               FLAGS_bulk_insert = false;
               jumpmu_return nullptr;
            }
            iterator = &bTreeLSMIterator;
         } else if (bTreeLSMIterator.done) {
            iterator = &aTreeLSMIterator;
         } else {  // take smaller element
            keyA->resize(0);
            unsigned keyLenA = bufferKey(aTreeLSMIterator, keyA.obj);
            keyB->resize(0);
            unsigned keyLenB = bufferKey(bTreeLSMIterator, keyB.obj);

            int compareValue = cmpKeys(keyA->data(), keyB->data(), keyLenA, keyLenB);
            if (compareValue == 0) {
               // both keys have the same key; this can happen through an update or an deletion
               // keyA is the newer key (higher LSM tree tier)
               if (aTreeLSMIterator.getDeletionFlag()) {
                  // key was in fact deleted, but we need the deletion entry for further levels where the original key may occur again!
                  // only if bTree is the lowest tier of the lsm tree we can forget the deletion marker and delete the entry really :)
                  if (bTree->level == this->tiers.size() - 1) {
                     aTreeLSMIterator.nextKV();
                  }
                  // could also test the bloom filters of further levels or do a lookup if the key occurs in a lower level, then we could delete the entry as well
               }
               // b is in every case the older value, isnt needed anymore (regardless if its a deleted entry, an updated entry or an originally inserted entry)
               bTreeLSMIterator.nextKV();
               continue;
            } else {
               iterator = compareValue < 0 ? &aTreeLSMIterator : &bTreeLSMIterator;
            }
         }
         keyA->resize(0);
         unsigned keyLen = bufferKey(*iterator, keyA.obj);
         // cout << "Key: " << (__builtin_bswap32(*reinterpret_cast<const uint32_t*>(keyA->data())) ^ (1ul << 31)) << endl;
         while (true) {
            jumpmuTry() {
               levelToReplace->tree.insertWithDeletionMarker(keyA->data(), keyLen, iterator->getPayload(), iterator->getPayloadLen(),
                                                             iterator->getDeletionFlag());
               jumpmu_break;
            }
            jumpmuCatch() { }
         }
         entryCount++;
         hashes->push_back(BloomFilter::hashKey(keyA->data(), iterator->getFullKeyLen()));
         iterator->nextKV();
      }
   }
   jumpmuCatch() { }
}
*/

// aTree is the root node (not the meta node!) of the first tree, bTree the root node of the second tree (may be null)
unique_ptr<StaticBTree> LSM::mergeTrees(unique_ptr<StaticBTree>& levelToReplace, btree::BTreeLL* aTree, btree::BTreeLL* bTree)
{
   auto newTree = make_unique<StaticBTree>();

   // a new merged tree is always on disk, never a In-memory tree
   newTree->tree.type = LSM_TYPE::BTree;
   newTree->filter.type = LSM_TYPE::BloomFilter;

   //dsi needed for root bTreeNode
   DataStructureIdentifier dsi = DataStructureIdentifier();
   dsi.type = LSM_TYPE::BTree;

   if (bTree == nullptr && aTree->type == LSM_TYPE::InMemoryBTree) { // we create the first level on disk, so the new merged tree has level 0 on disk
      newTree->tree.level = 0;
      newTree->filter.level = 0;
      dsi.level = 0;
   }
   else if (bTree == nullptr && aTree->type==LSM_TYPE::BTree) {
      newTree->tree.level = aTree->level + 1;
      newTree->filter.level = aTree->level + 1;
      dsi.level = aTree->level + 1;
   }
   else { // aTree is to large for level i, so its merged into bTree which resides at level i+1
      newTree->tree.level = bTree->level;
      newTree->filter.level = bTree->level;
      dsi.level = bTree->level;
   }

   jumpmuTry()
   {
      while (true) {
         jumpmuTry()
         {
            // create new meta_node (could even pick the meta_node of aTree or bTree, but here we only know the root of these two trees, not the meta node)
            auto& newMetaNode = BMC::global_bf->allocatePage();
            Guard guard(newMetaNode.header.latch, GUARD_STATE::EXCLUSIVE);
            newMetaNode.header.keep_in_memory = true;
            newMetaNode.page.dt_id = this->dt_id;
            ((btree::BTreeNode*)newMetaNode.page.dt)->type = dsi.type;
            ((btree::BTreeNode*)newMetaNode.page.dt)->level = dsi.level;
            guard.unlock();

            newTree->tree.create(this->dt_id, &newMetaNode, &dsi);

            HybridPageGuard<btree::BTreeNode> metaNode = HybridPageGuard<btree::BTreeNode>(newTree->tree.meta_node_bf);
            HybridPageGuard<btree::BTreeNode> rootNode = HybridPageGuard(metaNode, metaNode->upper);
            rootNode.toExclusive();
            rootNode->is_leaf = false;

            if(bTree == nullptr) {
               // we need a dummy node (upper ptr)
               HybridPageGuard<btree::BTreeNode> pg = HybridPageGuard<btree::BTreeNode>(aTree->meta_node_bf);
               HybridPageGuard<btree::BTreeNode> rootPg = HybridPageGuard(pg, pg->upper);
               rootPg.toExclusive();
               rootNode->upper = rootPg.swip();
               rootPg.bf->header.keep_in_memory = true;
               rootPg.unlock();
               pg.unlock();
            }
            else {
               // bTree is the tree at level i+1, which will be replaced. Set its root as upper "dummy node" of the new root
               HybridPageGuard<btree::BTreeNode> pg = HybridPageGuard<btree::BTreeNode>(bTree->meta_node_bf);
               HybridPageGuard<btree::BTreeNode> rootPg = HybridPageGuard(pg, pg->upper);
               rootPg.toExclusive();
               rootNode->upper = rootPg.swip();
               // we need to set the old root as keepInMemory, because two swips point to this node (meta_node and now the root of the new tree)
               rootPg.bf->header.keep_in_memory = true;
               rootPg.unlock();
               pg.unlock();
            }
            rootNode.unlock();
            metaNode.unlock();
            jumpmu_break;
         }
         jumpmuCatch() { }
      }
      newTree->tree.pageCount = 2; //root node + upper dummy node (does not contain the pages of the old tree)
      newTree->tree.height = 2; //root node + upper dummy node/old tree root

      merge_mutex.lock();
      bTreeInMerge = std::move(levelToReplace);
      if (bTree != nullptr)
         newTree->filter = std::move(bTreeInMerge->filter);
      levelToReplace = std::move(newTree);
      merge_mutex.unlock();

      uint64_t entryCount = 0;

      unsigned totalKeySize = 1;
      unsigned totalPayloadSize = 0;
      // saves the keyOffset and the length of the key and the payloadOffset and the length of the payload
      JMUW<vector<KeyValueEntry>> kvEntries;
      // saves the complete keys in one big list
      JMUW<vector<uint8_t>> keyStorage;
      // saves the payload in one big list
      JMUW<vector<uint8_t>> payloadStorage;
      keyStorage->reserve(btree::btreePageSize * 2);
      payloadStorage->reserve(btree::btreePageSize * 2);

      LSMBTreeMerger aTreeLSMIterator(*aTree);
      LSMBTreeMerger bTreeLSMIterator(*bTree);
      LSMBTreeMerger* iterator;

      aTreeLSMIterator.moveToFirstLeaf();
      if (bTree == nullptr) {
         bTreeLSMIterator.done = true;
      }
      else {
         bTreeLSMIterator.moveToFirstLeaf();
      }

      JMUW<vector<uint8_t>> keyA, keyB;
      keyA->reserve(btree::btreePageSize);
      keyB->reserve(btree::btreePageSize);

      JMUW<vector<uint64_t>> hashes;

      bool lowestNode = true;
      u16 lowerFenceLength = 0;

      kvEntries->push_back({0, 0, 0, 0, false});
      keyStorage->insert(keyStorage->end(), 0);

      while (true) {
         if (aTreeLSMIterator.done) {
            if (bTreeLSMIterator.done) {
               // all entries processed, create last page(s) and exit
               entryCount += kvEntries->size();

               //check if the remaining values fit in one page without a prefix (totalKeySize includes lowerFence, upperFenceLength is 0)
               if (spaceNeeded(0, totalKeySize, totalPayloadSize, kvEntries->size(), kvEntries->front().keyLen + kvEntries->back().keyLen) >= btree::btreePageSize) {
                  //save one(highest) key for last node and remove it then
                  auto entryForLastNode = kvEntries->back();
                  uint8_t* key = keyStorage->data() + keyStorage->size() - entryForLastNode.keyLen;
                  uint8_t* payload = payloadStorage->data() + payloadStorage->size() - entryForLastNode.payloadLen;
                  kvEntries->pop_back();

                  buildNode(kvEntries, keyStorage, payloadStorage, levelToReplace->tree, lowestNode, false);
                  lowerFenceLength = kvEntries->back().keyLen;
                  JMUW<vector<uint8_t>> lowerFenceMerker;
                  lowerFenceMerker->reserve(kvEntries->back().keyLen);
                  lowerFenceMerker->insert(lowerFenceMerker->end(), (kvEntries->back().keyOffset + keyStorage->data()), (kvEntries->back().keyOffset + keyStorage->data()) + lowerFenceLength);

                  kvEntries->clear();
                  keyStorage->clear();
                  payloadStorage->clear();

                  //lower fence for last node
                  kvEntries->push_back({0, lowerFenceLength, 0, 0, false});
                  keyStorage->insert(keyStorage->end(), lowerFenceMerker->data(), lowerFenceMerker->data() + lowerFenceLength);
                  // first and last entry for last node
                  unsigned keyOffset = keyStorage->end() - keyStorage->begin();
                  unsigned payloadOffset = payloadStorage->end() - payloadStorage->begin();
                  kvEntries->push_back({keyOffset, entryForLastNode.keyLen, payloadOffset, entryForLastNode.payloadLen, entryForLastNode.deletionFlag});
                  keyStorage->insert(keyStorage->end(), key, key + entryForLastNode.keyLen);
                  payloadStorage->insert(payloadStorage->end(), payload, payload + entryForLastNode.payloadLen);
               }


               buildNode(kvEntries, keyStorage, payloadStorage, levelToReplace->tree, lowestNode, true);

               assert(((btree::BTreeNode*)((btree::BTreeNode*)levelToReplace->tree.meta_node_bf->page.dt)->upper.bf->page.dt)->type == LSM_TYPE::BTree);

               // create BloomFilter
               //levelToReplace->filter->init(entryCount);
               DataStructureIdentifier bloomDSI = DataStructureIdentifier();
               bloomDSI.level = dsi.level;
               bloomDSI.type = LSM_TYPE::BloomFilter;
               levelToReplace->filter.create(this->dt_id, &bloomDSI, entryCount);
               for (auto h : hashes.obj)
                  levelToReplace->filter.insert(h);

               // merge done

               jumpmuTry()
               {
                  // Reclaim the remaining meta & root nodes
                  HybridPageGuard<btree::BTreeNode> aTreeMetaNode = HybridPageGuard<btree::BTreeNode>(aTree->meta_node_bf);
                  HybridPageGuard<btree::BTreeNode> aTreeRootNode(aTreeMetaNode, aTreeMetaNode->upper);
                  aTreeRootNode.toExclusive();
                  aTreeMetaNode.toExclusive();
                  aTreeRootNode.reclaim();
                  aTreeMetaNode.reclaim();
                  if (bTree != nullptr) {
                     HybridPageGuard<btree::BTreeNode> bTreeMetaNode = HybridPageGuard<btree::BTreeNode>(bTree->meta_node_bf);
                     HybridPageGuard<btree::BTreeNode> bTreeRootNode(bTreeMetaNode, bTreeMetaNode->upper);
                     bTreeRootNode.toExclusive();
                     bTreeMetaNode.toExclusive();
                     bTreeRootNode.reclaim();
                     bTreeMetaNode.reclaim();
                  }
               }
               jumpmuCatch() { }

               assert(((btree::BTreeNode*)((btree::BTreeNode*)levelToReplace->tree.meta_node_bf->page.dt)->upper.bf->page.dt)->type == LSM_TYPE::BTree);

               jumpmu_return nullptr;
            }
            iterator = &bTreeLSMIterator;
         } else if (bTreeLSMIterator.done) {
            iterator = &aTreeLSMIterator;
         } else {  // take smaller element
            keyA->resize(0);
            bufferKey(aTreeLSMIterator, keyA.obj);
            keyB->resize(0);
            bufferKey(bTreeLSMIterator, keyB.obj);
            int compareValue = cmpKeys(keyA->data(), keyB->data(), keyA->size(), keyB->size());
            if (compareValue == 0) {
               // both keys have the same key; this can happen through an update or an deletion
               // keyA is the newer key (higher LSM tree tier)
               if (aTreeLSMIterator.getDeletionFlag()) {
                  // key was in fact deleted, but we need the deletion entry for further levels where the original key may occur again!
                  // only if bTree is the lowest tier of the lsm tree we can forget the deletion marker and delete the entry really :)
                  if (bTree->level == this->tiers.size() - 1) {
                     aTreeLSMIterator.nextKV();
                  }
                  // could also test the bloom filters of further levels or do a lookup if the key occurs in a lower level, then we could delete the entry as well
               }
               // b is in every case the older value, isnt needed anymore (regardless if its a deleted entry, an updated entry or an originally inserted entry)
               bTreeLSMIterator.nextKV();
               continue;
            } else {
               iterator = compareValue < 0 ? &aTreeLSMIterator : &bTreeLSMIterator;
            }
         }
         unsigned keyOffset = keyStorage->end() - keyStorage->begin();
         unsigned keyLen = bufferKey(*iterator, keyStorage.obj);
         unsigned payloadOffset = payloadStorage->end() - payloadStorage->begin();
         unsigned payloadLen = bufferPayload(*iterator, payloadStorage.obj);
         bool deletionFlag = iterator->getDeletionFlag();

         kvEntries->push_back({keyOffset, keyLen, payloadOffset, payloadLen, deletionFlag});

         uint8_t* key = keyStorage->data() + keyStorage->size() - keyLen;
         // uint8_t* payload = payloadStorage.data() + payloadStorage.size() - payloadLen;
         hashes->push_back(BloomFilter::hashKey(key, keyLen));

         unsigned prefix = commonPrefix(kvEntries->front().keyOffset + keyStorage->data(), kvEntries->front().keyLen, key, keyLen);
         if (lowestNode) {
            prefix = 0;
         }
         if (spaceNeeded(prefix, totalKeySize + keyLen, totalPayloadSize + payloadLen, kvEntries->size(), kvEntries->front().keyLen + keyLen) >= btree::btreePageSize) {
            // key does not fit, create new page
            kvEntries->pop_back();
            entryCount += kvEntries->size();
            if (kvEntries->front().keyLen == kvEntries->back().keyLen)
               assert(std::memcmp((kvEntries->back().keyOffset + keyStorage->data()), (kvEntries->front().keyOffset + keyStorage->data()), kvEntries->front().keyLen) != 0);
            buildNode(kvEntries, keyStorage, payloadStorage, levelToReplace->tree, lowestNode, false);  // could pick shorter separator here
            lowerFenceLength = kvEntries->back().keyLen;
            lowestNode = false;

            JMUW<vector<uint8_t>> lowerFenceMerker;
            lowerFenceMerker->reserve(kvEntries->back().keyLen);
            lowerFenceMerker->insert(lowerFenceMerker->end(), (kvEntries->back().keyOffset + keyStorage->data()), (kvEntries->back().keyOffset + keyStorage->data()) + lowerFenceLength);

            kvEntries->clear();
            keyStorage->clear();
            payloadStorage->clear();
            totalKeySize = 0;
            totalPayloadSize = 0;

            kvEntries->push_back({0, lowerFenceLength, 0, 0, false});
            keyStorage->insert(keyStorage->end(), lowerFenceMerker->data(), lowerFenceMerker->data() + lowerFenceLength);
            totalKeySize += lowerFenceLength;

         } else {
            // key fits, buffer it
            // go to next entry in the tree
            iterator->nextKV();
            totalKeySize += keyLen;
            totalPayloadSize += payloadLen;
         }
      }
   }
   jumpmuCatch(){ }
}
LSM::LSM() : inMemBTree(std::make_unique<btree::BTreeLL>()) {inMemBTree->type=LSM_TYPE::InMemoryBTree; inMemBTree->level = 0;}

LSM::~LSM() {}

// create an LSM Tree with DTID dtid; meta information is in meta_bf
void LSM::create(DTID dtid, BufferFrame* meta_bf)
{
   this->meta_node_bf = meta_bf;
   this->dt_id = dtid;
   this->inMemBTree = make_unique<btree::BTreeLL>();

   // create the inMemory BTree
   DataStructureIdentifier dsi = DataStructureIdentifier();
   dsi.type = LSM_TYPE::InMemoryBTree;
   dsi.level = 0;

   while (true) {
      jumpmuTry()
      {
         // create new metaPage for the In-memory BTree
         auto& newMetaBufferPage = BMC::global_bf->allocatePage();
         Guard guard(newMetaBufferPage.header.latch, GUARD_STATE::EXCLUSIVE);
         newMetaBufferPage.header.keep_in_memory = true;
         newMetaBufferPage.page.dt_id = dtid;
         // set the dsi for the metaNode
         ((btree::BTreeNode*)newMetaBufferPage.page.dt)->type = dsi.type;
         ((btree::BTreeNode*)newMetaBufferPage.page.dt)->level = dsi.level;
         guard.unlock();

         // through the dsi.type=InMemBTree the create function of the GenericBTree creates pages with keepInMemory=true
         this->inMemBTree->create(dtid, &newMetaBufferPage, &dsi);
         jumpmu_break;
      }
      jumpmuCatch() {  }
   }

   // set the dsi for the InMemBTree of the LSM-Tree
   this->inMemBTree->type = dsi.type;
   this->inMemBTree->level = dsi.level;
}

void LSM::printLevels()
{
   uint64_t sum = 0;
   for (auto& t : tiers) {
      sum += t->tree.pageCount;
      cout << t->tree.pageCount << " ";
   }
   cout << " sum:" << sum << endl;
}

// merges
void LSM::mergeAll()
{
   uint64_t limit = baseLimit * factor;
   for (unsigned i = 0; i < tiers.size(); i++) {
      // curr = Btree on level i
      StaticBTree& curr = *tiers[i];
      if (curr.tree.pageCount >= limit) {
         // current observed BTree is too large
         this->levelInMerge = i+1;
         if (i + 1 < tiers.size()) {
            // merge with next level
            cout << "merge level " << i << " with level " << i+1 << endl;

            mergeTrees(tiers[i + 1], &curr.tree, &tiers[i + 1]->tree);
            bTreeInMerge.release();
            cout << "Merge done" << endl;

            ensure(tiers[i+1]->tree.type == LSM_TYPE::BTree);
            ensure(tiers[i+1]->filter.type == LSM_TYPE::BloomFilter);
            ensure(tiers[i+1]->tree.level == i+1);
            ensure(tiers[i+1]->filter.level == i+1);
         } else {
            // new level
            tiers.emplace_back(nullptr);
            mergeTrees(tiers[i + 1], &curr.tree, nullptr);

            //cout << "neues level, jetzt: " << i+1 << " DTID: " << this->dt_id << " countEntries LSM: " << this->countEntries() <<endl;
            tiers[i+1]->tree.type = LSM_TYPE::BTree;
            tiers[i+1]->tree.level = i+1;
            tiers[i+1]->filter.type = LSM_TYPE::BloomFilter;
            tiers[i+1]->filter.level = i+1;
            ((btree::BTreeNode*)tiers[i+1]->tree.meta_node_bf->page.dt)->type = LSM_TYPE::BTree;
            ((btree::BTreeNode*)tiers[i+1]->tree.meta_node_bf->page.dt)->level = i+1;
         }

         HybridPageGuard<BloomFilter::BloomFilterPage> rootBloomFilterPage = HybridPageGuard<BloomFilter::BloomFilterPage>(tiers[i]->filter.rootBloomFilterPage);
         tiers[i]->filter.deleteBloomFilterPages(rootBloomFilterPage);

         tiers[i] = make_unique<StaticBTree>();

         DataStructureIdentifier dsi = DataStructureIdentifier();
         dsi.type = LSM_TYPE::BTree;
         dsi.level = i;

         while (true) {
            jumpmuTry()
            {
               // create new metaPage for the tiers[i] BTree
               auto& newMetaBufferPage = BMC::global_bf->allocatePage();
               Guard guard(newMetaBufferPage.header.latch, GUARD_STATE::EXCLUSIVE);
               newMetaBufferPage.header.keep_in_memory = true;
               newMetaBufferPage.page.dt_id = this->dt_id;
               ((btree::BTreeNode*)newMetaBufferPage.page.dt)->type = dsi.type;
               ((btree::BTreeNode*)newMetaBufferPage.page.dt)->level = dsi.level;
               guard.unlock();

               // create btree for level tiers[i]
               tiers[i]->tree.create(this->dt_id, &newMetaBufferPage, &dsi);
               tiers[i]->tree.type = dsi.type;
               tiers[i]->tree.level = dsi.level;
               tiers[i]->filter.type = LSM_TYPE::BloomFilter;
               tiers[i]->filter.level = i;
               jumpmu_break;
            }
            jumpmuCatch() { }
         }
      }
      limit = limit * factor;
   }
}


/*
// returns true when the key is found in the BloomFilter or in the inMemBTree
bool LSM::lookupOnlyBloomFilter(uint8_t* key, unsigned keyLength)
{
   if (inMemBTree->lookup(key, keyLength))
      return true;

   for (unsigned i = 0; i < tiers.size(); i++) {
      if (tiers[i]->filter.lookup(key, keyLength))
         return true;
   }
   return false;
}*/


OP_RESULT LSM::updateSameSizeInPlace(u8* key, u16 keyLength, function<void(u8* payload, u16 payloadSize)> callback)
{
   u8* keyPayload;
   u16 keyPayloadLength;

   // case1: record to update is already in the inMemTree
   //      case1.1: record is marked as deleted -> NOT FOUND
   //      case1.2: normal record (because of insert or earlier update) -> updateSameSizeInPlace
   // case2: record is in tree(s) below
   //      case2.1: newest record is marked a deleted -> NOT FOUND
   //      case2.2: newest record is not marked as deleted -> insert new record in inMemTree
   // case3: record not found -> NOT FOUND

   //search only the inMemTree to get the information when the entry is marked as deleted
   OP_RESULT occursInInMemTree = this->inMemBTree->lookup(key, keyLength,[&](const u8* payload, u16 payload_length) {
     static_cast<void>(payload_length);
     u8& typed_payload = *const_cast<u8*>(reinterpret_cast<const u8*>(payload));
     keyPayload = &typed_payload;
     keyPayloadLength = payload_length;
   });

   if(occursInInMemTree == OP_RESULT::OK) {
      //case1.2
      //therefore do traditional update in the inMemBTree
      return this->inMemBTree->updateSameSizeInPlace(key, keyLength, callback);
   }
   else if (occursInInMemTree == OP_RESULT::LSM_DELETED) {
      //case1.1
      return OP_RESULT::NOT_FOUND;
   }
   else {
      //case2 + case3
      //lookup in complete LSM Tree (LSM_DELETED is hidden, we get only OK or NOT_FOUND)
      OP_RESULT occursInLSMTree = LSM::lookup(key, keyLength,[&](const u8* payload, u16 payload_length) {
        static_cast<void>(payload_length);
        u8& typed_payload = *const_cast<u8*>(reinterpret_cast<const u8*>(payload));
        keyPayload = &typed_payload;
        keyPayloadLength = payload_length;
      });

      if (occursInLSMTree == OP_RESULT::NOT_FOUND) {
         //case2.1 + case3
         return OP_RESULT::NOT_FOUND;
      }
      else {
         //case2.2
         //second: update the values through the callback function
         callback(keyPayload, keyPayloadLength);

         //third: insert the updated values in the LSMTree (respectively InMemBTree)
         return LSM::insertWithDeletionMarkerUpdateMarker(key, keyLength, keyPayload, keyPayloadLength, false, true);
      }
   }
}

OP_RESULT LSM::remove(u8* key, u16 keyLength)
{
   // case1: record to delete is only in the inMemTree (because of insert or earlier update) -> would result in Duplicate Key inMemTree, could be solved with a traditional delete or with a deletion-marker
   // case2: record to delete is in the inMemTree and in further levels (because of insert or earlier update) -> would result in Duplicate Key in the inMemTree -> update entry with deletion-marker
   // case3: record is only in a tree below
   //      case3.1: newest record in the tiers is marked as deleted -> NOT FOUND
   //      case3.2: newest record in the tiers is a normal entry -> insert deletionMarker in inMemTree
   // case4: record to delete is in the inMemTree, but there already marked as deleted -> NOT FOUND

   //lookup only in inMemBTree to get the LSM_DELETED entry (would be hidden in LSM::lookup)
   OP_RESULT occursInInMemTree = this->inMemBTree->lookup(key, keyLength,[&](const u8*, u16) {});

   if (occursInInMemTree == OP_RESULT::LSM_DELETED) {
      //case4
      return OP_RESULT::NOT_FOUND;
   }
   else if (occursInInMemTree == OP_RESULT::NOT_FOUND) {
      //case3
      //insert the key with deletion marker without any payload

      // lookup in complete LSM Tree (LSM_DELETED is hidden, we get only OK or NOT_FOUND)
      if (this->lookup(key, keyLength, [&](const u8*,u16){}) == OP_RESULT::OK) {
         // occurs in one level below and is not deleted yet
         return insertWithDeletionMarker(key, keyLength, nullptr, 0, true);
      }
      else {
         // not found in lower levels or even marked as deleted in a lower level
         return OP_RESULT::NOT_FOUND;
      }
   }
   else {
      //case1 or case2, occurs in InMemTree
      //instead of differentiating between these two cases we simply set the deletion marker

      //set the deletion marker for the entry in the inMemBTree
      return this->inMemBTree->removeWithDeletionMarker(key, keyLength);
   }
}

// scans ascending beginning with start_key
// callback function should return false when scanned the desired range and true when the scan should go further
OP_RESULT LSM::scanAsc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
{
   //TODO: Scan all trees parallel
   Slice searchKey(start_key, key_length);

   jumpmuTry()
   {
      btree::BTreeSharedIterator inMemBTreeIterator(*static_cast<btree::BTreeLL*>(inMemBTree.get()));
      bool inMemMoveNext;
      Slice inMemSlice;

      btree::BTreeSharedIterator tierIterators[tiers.size()];
      for (unsigned i = 0; i < tiers.size(); i++) {
         // initialize
         tierIterators[i] = btree::BTreeSharedIterator(*static_cast<btree::BTreeLL*>(&tiers[i]->tree));
      }
      bool tiersMoveNext[tiers.size()];
      Slice tierSlices[tiers.size()];

      //************** Initialization *****************
      inMemMoveNext = false;
      auto ret = inMemBTreeIterator.seek(searchKey);
      if (ret != OP_RESULT::OK) {
         // no suitable start value found in the inMemBTree
         inMemSlice = Slice(reinterpret_cast<const unsigned char*>(""),0);
      } else {
         inMemSlice = Slice(inMemBTreeIterator.key().data(), inMemBTreeIterator.key().size());
         // maybe the first value found is deleted, search further
         while (inMemBTreeIterator.leaf->isDeleted(inMemBTreeIterator.cur)) {
            auto ret = inMemBTreeIterator.next();
            if (ret != OP_RESULT::OK) {
               // no suitable start value found in the inMemBTree, move on to the tiers
               inMemSlice = Slice(reinterpret_cast<const unsigned char*>(""),0);
               break;
            }
            else {
               inMemSlice = Slice(inMemBTreeIterator.key().data(), inMemBTreeIterator.key().size());
            }
         }
      }


      for (unsigned i = 0; i < tiers.size(); i++) {
         // check levels
         tiersMoveNext[i] = false;
         auto ret = tierIterators[i].seek(searchKey);
         if (ret != OP_RESULT::OK) {
            // no suitable start value found in this level
            tierSlices[i] = Slice(reinterpret_cast<const unsigned char*>(""), 0);
         } else {
            tierSlices[i] = Slice(tierIterators[i].key().data(), tierIterators[i].key().size());
            // maybe the first value which was found is deleted, search further
            while (tierIterators[i].leaf->isDeleted(tierIterators[i].cur)) {
               auto ret = tierIterators[i].next();
               if (ret != OP_RESULT::OK) {
                  // no suitable start value found in this tier, move on to next tier
                  tierSlices[i] = Slice(reinterpret_cast<const unsigned char*>(""), 0);
                  break;
               } else {
                  tierSlices[i] = Slice(tierIterators[i].key().data(), tierIterators[i].key().size());
               }
            }
         }
      }

      //*************** Loop for searching next higher keys ****************
      while (true) {
         if (inMemMoveNext) {
            // checkInMemBtree
            inMemMoveNext = false;
            auto ret = inMemBTreeIterator.next();
            if (ret != OP_RESULT::OK) {
               // no suitable value found in the inMemBTree
               inMemSlice = Slice(reinterpret_cast<const unsigned char*>(""), 0);
            } else {
               inMemSlice = Slice(inMemBTreeIterator.key().data(), inMemBTreeIterator.key().size());
               // maybe this value is deleted, search further
               while (inMemBTreeIterator.leaf->isDeleted(inMemBTreeIterator.cur)) {
                  auto ret = inMemBTreeIterator.next();
                  if (ret != OP_RESULT::OK) {
                     // no next suitable value found in the inMemBTree
                     inMemSlice = Slice(reinterpret_cast<const unsigned char*>(""),0);
                     break;
                  }
                  else {
                     inMemSlice = Slice(inMemBTreeIterator.key().data(), inMemBTreeIterator.key().size());
                  }
               }
            }
         }

         for (unsigned i = 0; i < tiers.size(); i++) {
            if (tiersMoveNext[i]) {
               // check levels
               tiersMoveNext[i] = false;
               auto ret = tierIterators[i].next();
               if (ret != OP_RESULT::OK) {
                  // no suitable value found in this level
                  tierSlices[i] = Slice(reinterpret_cast<const unsigned char*>(""), 0);
               } else {
                  tierSlices[i] = Slice(tierIterators[i].key().data(), tierIterators[i].key().size());
                  // maybe the next value in this tier is deleted, search further
                  while (tierIterators[i].leaf->isDeleted(tierIterators[i].cur)) {
                     auto ret = tierIterators[i].next();
                     if (ret != OP_RESULT::OK) {
                        // no suitable start value found in this tier, move on to next tier
                        tierSlices[i] = Slice(reinterpret_cast<const unsigned char*>(""), 0);
                        break;
                     } else {
                        tierSlices[i] = Slice(tierIterators[i].key().data(), tierIterators[i].key().size());
                     }
                  }
               }
            }
         }

         //************ Find smallest value *****************

         // smallest Key in Level (-1 = inMemBTree, 0 = tiers[0])
         int smallestValue = -1;
         Slice nextValue = inMemSlice;

         for (unsigned i = 0; i < tiers.size(); i++) {
            // eliminate old entries when the key is the same
            // get the lowest key and call the callback function
            // next() for:
            //    iterator with lowest key
            //    iterator with duplicates (old entries)
            // no next() for those who:
            //    already havent found a suitable value
            //    have found a value but theirs wasnt the lowest value

            if (tierSlices[i].empty()) {
               continue;
            }

            if (nextValue.empty()) {
               // no value found in the levels before
               nextValue = tierSlices[i];
               smallestValue = i;
            } else if (cmpKeysString(tierSlices[i].data(), nextValue.data(), tierSlices[i].size(), nextValue.size()) < 0) {
               // lower key found
               nextValue = tierSlices[i];
               smallestValue = i;
            } else if (cmpKeysString(tierSlices[i].data(), nextValue.data(), tierSlices[i].size(), nextValue.size()) == 0) {
               // same key found (old version), can move to next entry in this tier
               tiersMoveNext[i] = true;
            } else {
               // key in level in larger than the smallest key, may be evaluated in one of the next loops
            }
         }

         //************* Evaluation of the lowest key **************
         if (inMemSlice.empty() && smallestValue == -1) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         } else if (smallestValue == -1) {
            Slice key = inMemBTreeIterator.key();
            Slice value = inMemBTreeIterator.value();
            if (!callback(key.data(), key.length(), value.data(), value.length())) {
               jumpmu_return OP_RESULT::OK;
            } else {
               inMemMoveNext = true;
            }
         } else {
            Slice key = tierIterators[smallestValue].key();
            Slice value = tierIterators[smallestValue].value();
            if (!callback(key.data(), key.length(), value.data(), value.length())) {
               jumpmu_return OP_RESULT::OK;
            } else {
               tiersMoveNext[smallestValue] = true;
            }
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}

OP_RESULT LSM::scanDesc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
{
   //TODO: Scan all trees parallel
   Slice searchKey(start_key, key_length);

   jumpmuTry()
   {
      btree::BTreeSharedIterator inMemBTreeIterator(*static_cast<btree::BTreeLL*>(inMemBTree.get()));
      bool inMemMoveNext;
      Slice inMemSlice;

      btree::BTreeSharedIterator tierIterators[tiers.size()];
      for (unsigned i = 0; i < tiers.size(); i++) {
         // initialize
         tierIterators[i] = btree::BTreeSharedIterator(*static_cast<btree::BTreeLL*>(&tiers[i]->tree));
      }
      bool tiersMoveNext[tiers.size()];
      Slice tierSlices[tiers.size()];

      //************** Initialization *****************
      inMemMoveNext = false;
      auto ret = inMemBTreeIterator.seekForPrev(searchKey);
      if (ret != OP_RESULT::OK) {
         // no suitable value found in the inMemBTree
         inMemSlice = Slice(reinterpret_cast<const unsigned char*>(""), 0);
      } else {
         inMemSlice = Slice(inMemBTreeIterator.key().data(), inMemBTreeIterator.key().size());
         // maybe the first value found is deleted, search further
         while (inMemBTreeIterator.leaf->isDeleted(inMemBTreeIterator.cur)) {
            auto ret = inMemBTreeIterator.prev();
            if (ret != OP_RESULT::OK) {
               // no suitable start value found in the inMemBTree, move on to the tiers
               inMemSlice = Slice(reinterpret_cast<const unsigned char*>(""),0);
               break;
            }
            else {
               inMemSlice = Slice(inMemBTreeIterator.key().data(), inMemBTreeIterator.key().size());
            }
         }
      }

      for (unsigned i = 0; i < tiers.size(); i++) {
         // check levels
         tiersMoveNext[i] = false;
         auto ret = tierIterators[i].seekForPrev(searchKey);
         if (ret != OP_RESULT::OK) {
            // no suitable value found in this level
            tierSlices[i] = Slice(reinterpret_cast<const unsigned char*>(""), 0);
         } else {
            tierSlices[i] = Slice(tierIterators[i].key().data(), tierIterators[i].key().size());
            // maybe the first value which was found is deleted, search further
            while (tierIterators[i].leaf->isDeleted(tierIterators[i].cur)) {
               auto ret = tierIterators[i].prev();
               if (ret != OP_RESULT::OK) {
                  // no suitable start value found in this tier, move on to next tier
                  tierSlices[i] = Slice(reinterpret_cast<const unsigned char*>(""), 0);
                  break;
               } else {
                  tierSlices[i] = Slice(tierIterators[i].key().data(), tierIterators[i].key().size());
               }
            }
         }
      }

      //*************** Loop for searching further Keys ****************
      while (true) {
         if (inMemMoveNext) {
            // checkInMemBtree
            inMemMoveNext = false;
            auto ret = inMemBTreeIterator.prev();
            if (ret != OP_RESULT::OK) {
               // no suitable value found in the inMemBTree
               inMemSlice = Slice(reinterpret_cast<const unsigned char*>(""), 0);
            } else {
               inMemSlice = Slice(inMemBTreeIterator.key().data(), inMemBTreeIterator.key().size());
               // maybe this value is deleted, search further
               while (inMemBTreeIterator.leaf->isDeleted(inMemBTreeIterator.cur)) {
                  auto ret = inMemBTreeIterator.prev();
                  if (ret != OP_RESULT::OK) {
                     // no next suitable value found in the inMemBTree
                     inMemSlice = Slice(reinterpret_cast<const unsigned char*>(""),0);
                     break;
                  }
                  else {
                     inMemSlice = Slice(inMemBTreeIterator.key().data(), inMemBTreeIterator.key().size());
                  }
               }
            }
         }

         for (unsigned i = 0; i < tiers.size(); i++) {
            if (tiersMoveNext[i]) {
               // check levels
               tiersMoveNext[i] = false;
               auto ret = tierIterators[i].prev();
               if (ret != OP_RESULT::OK) {
                  // no suitable value found in this level
                  tierSlices[i] = Slice(reinterpret_cast<const unsigned char*>(""), 0);
               } else {
                  tierSlices[i] = Slice(tierIterators[i].key().data(), tierIterators[i].key().size());
                  // maybe the next value in this tier is deleted, search further
                  while (tierIterators[i].leaf->isDeleted(tierIterators[i].cur)) {
                     auto ret = tierIterators[i].prev();
                     if (ret != OP_RESULT::OK) {
                        // no suitable start value found in this tier, move on to next tier
                        tierSlices[i] = Slice(reinterpret_cast<const unsigned char*>(""), 0);
                        break;
                     } else {
                        tierSlices[i] = Slice(tierIterators[i].key().data(), tierIterators[i].key().size());
                     }
                  }
               }
            }
         }

         //************ Find highest value *****************

         // highest Key in Level (-1 = inMemBTree, 0 = tiers[0])
         int64_t highestValue = -1;
         Slice nextValue = inMemSlice;

         for (unsigned i = 0; i < tiers.size(); i++) {
            // eliminate old entries when the key is the same
            // get the lowest key and call the callback function
            // next() for:
            //    iterator with lowest key
            //    iterator with duplicates (old entries)
            // no next() for those who:
            //    already havent found a suitable value
            //    have found a value but theirs wasnt the lowest value

            if (tierSlices[i].empty()) {
               continue;
            }

            if (nextValue.empty()) {
               // no value found in the levels before
               nextValue = tierSlices[i];
               highestValue = i;
            } else if (cmpKeysString(tierSlices[i].data(), nextValue.data(), tierSlices[i].size(), nextValue.size()) > 0) {
               // larger key found
               nextValue = tierSlices[i];
               highestValue = i;
            } else if (cmpKeysString(tierSlices[i].data(), nextValue.data(), tierSlices[i].size(), nextValue.size()) == 0) {
               // same key found (old version), can move to next entry in this tier
               tiersMoveNext[i] = true;
            } else {
               // key in level in smaller than the highest key, may be evaluated in one of the next loops
            }
         }

         //************* Evaluation of the highest key **************
         if (inMemSlice.empty() && highestValue == -1) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         } else if (highestValue == -1) {
            auto key = inMemBTreeIterator.key();
            auto value = inMemBTreeIterator.value();
            if (!callback(key.data(), key.length(), value.data(), value.length())) {
               jumpmu_return OP_RESULT::OK;
            } else {
               inMemMoveNext = true;
            }
         } else {
            auto key = tierIterators[highestValue].key();
            auto value = tierIterators[highestValue].value();
            if (!callback(key.data(), key.length(), value.data(), value.length())) {
               jumpmu_return OP_RESULT::OK;
            } else {
               tiersMoveNext[highestValue] = true;
            }
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}

// returns the number of pages in the LSM Tree
u64 LSM::countPages()
{
   u64 pageCount = inMemBTree->pageCount;

   for (unsigned i = 0; i < tiers.size(); i++) {
      pageCount += tiers[i]->tree.pageCount;
   }

   return pageCount;
}

// returns the entries in the LSM Tree
u64 LSM::countEntries()
{
   u64 entryCount = inMemBTree->countEntries();

   for (unsigned i = 0; i < tiers.size(); i++) {
      entryCount += tiers[i]->tree.countEntries();
   }

   return entryCount;
}

// returns the level count of the LSM Tree
u64 LSM::getHeight()
{
   return this->tiers.size();
}

OP_RESULT LSM::insert(u8* key, u16 keyLength, u8* payload, u16 payloadLength)
{
   return insertWithDeletionMarker(key, keyLength, payload, payloadLength, false);
}
// inserts a value in the In-memory Level Tree of the LSM tree and merges with lower levels if necessary
OP_RESULT LSM::insertWithDeletionMarker(u8* key, u16 keyLength, u8* payload, u16 payloadLength, bool deletionMarker)
{
   return insertWithDeletionMarkerUpdateMarker(key, keyLength, payload, payloadLength, deletionMarker, false);
}
// inserts a value in the In-memory Level Tree of the LSM tree and merges with lower levels if necessary
OP_RESULT LSM::insertWithDeletionMarkerUpdateMarker(u8* key, u16 keyLength, u8* payload, u16 payloadLength, bool deletionMarker, bool updateMarker)
{
   // case1: key does not occur in the hole LSM Tree -> normal insert
   // case2: key occurs in the inMemBtree
   //      case2.1: key is a deletion entry -> duplicate key with normal insert, but should be ok (delete deleted entry + insert new entry -> in the end an "update", but maybe with different payload length)
   //      case2.2: non-deleted entry -> insert fails with duplicate key
   // case3: key occurs in a lower tier -> insert would be ok!! So check the further levels before if key occurs
   //      case3.1: key occurs in a next lower tier and not marked as deleted -> dont allow insert
   //      case3.2: key occurs in a next lower tier, but deletion marker is set in the first/newest lower tier -> could insert value again
   //            -> lower levels with the same key may exist but arent of interest (are deleted during merge)

   // if its an insert of an deletionMarker, we know that we can insert in the inMemBTree because we checked it in LSM::remove before
   if (!deletionMarker && !updateMarker) {
      // first lookup inMemTree
      OP_RESULT lookupResult = inMemBTree->lookup(key, keyLength, [&](const u8*, u16) {});
      if (lookupResult == OP_RESULT::OK) {
         // case2.2
         return OP_RESULT::DUPLICATE;
      } else if (lookupResult == OP_RESULT::LSM_DELETED) {
         // case2.1
         // real delete + insert
         lookupResult = inMemBTree->remove(key, keyLength);
         assert(lookupResult==OP_RESULT::OK);
      } else {
         // lookup tiers
         // optional: parallel lookup with queue
         for (unsigned i = 0; i < tiers.size(); i++) {
            if (!tiers[i]->filter.lookup(key, keyLength)) {
               // value does not occur in tiers[i]
               continue;
            }
            // value may occur in tiers[i] (may be a deleted entry, an updated entry or the original entry or a false positive in the Bloom Filter)

            lookupResult = tiers[i]->tree.lookup(key, keyLength, [&](const u8*, u16) {});
            if (lookupResult == OP_RESULT::OK) {
               // case3.1
               return OP_RESULT::DUPLICATE;
            } else if (lookupResult == OP_RESULT::LSM_DELETED) {
               // case3.2
               break;
            }
         }
      }
      // case1 + case3.2 + case2.1
   }

   auto result = inMemBTree->insertWithDeletionMarker(key, keyLength, payload, payloadLength, deletionMarker);
   u64 pageCount = inMemBTree->pageCount;
   if (pageCount >= baseLimit) {
      // merge inMemory-BTree with first level BTree of LSM Tree
      this->levelInMerge = 0;
      // move full inMemBTree to inMemBTreeInMerge
      this->inMemBTreeInMerge = std::move(inMemBTree);
      // generate new empty inMemory BTree
      this->create(this->dt_id, this->meta_node_bf);

      // merge the two trees / pull the in-mem-tree down (if only in-mem-tree exists)
      if (!tiers.empty()) {
         mergeTrees(tiers[0], inMemBTreeInMerge.get(), &tiers[0]->tree);
         inMemBTreeInMerge.release();
         bTreeInMerge.release();
         assert(((btree::BTreeNode*)((btree::BTreeNode*)tiers[0]->tree.meta_node_bf->page.dt)->upper.bf->page.dt)->type == LSM_TYPE::BTree);
      }
      else {
         // create first tier
         tiers.emplace_back(nullptr);
         mergeTrees(tiers[0], inMemBTreeInMerge.get(), nullptr);
      }

      ensure(tiers[0]->tree.type == LSM_TYPE::BTree);
      ensure(tiers[0]->filter.type == LSM_TYPE::BloomFilter);
      ensure(tiers[0]->tree.level == 0);
      ensure(tiers[0]->filter.level == 0);

      // if necessary merge further levels
      mergeAll();
   }
   return result;
}

// searches for an value in the LSM tree, returns OK or NOT_FOUND (hides LSM_DELETED)
OP_RESULT LSM::lookup(u8* key, u16 keyLength, function<void(const u8*, u16)> payload_callback)
{
   //case1: key found in inMemTree
   //     case1.1: with deletion marker -> Not found
   //     case1.2: normal entry -> OK
   //case2: not found in inMemTree, search further levels
   //     case2.1: found in one of the further levels with deletion marker -> Not found
   //     case2.2: found in one of the further levels -> OK

   OP_RESULT lookupResult = inMemBTree->lookup(key, keyLength, payload_callback);
   if (lookupResult == OP_RESULT::OK)
      return OP_RESULT::OK;
   else if (lookupResult == OP_RESULT::LSM_DELETED)
      return OP_RESULT::NOT_FOUND;

   // optional: parallel lookup with queue
   for (unsigned i = 0; i < tiers.size(); i++) {
      // TODO enable filter lookup again
      if (!tiers[i]->filter.lookup(key, keyLength)) {
         // value does not occur in tiers[i]
         continue;
      }
      // value may occur in tiers[i] (may be a deleted entry, an updated entry or the original entry or a false positive in the Bloom Filter)

      //if (tiers[i]->filter.lookup(key, keyLength) && tiers[i]->tree.lookup(key, keyLength,payload_callback) == OP_RESULT::OK)
      lookupResult = tiers[i]->tree.lookup(key, keyLength,payload_callback);
      if (lookupResult == OP_RESULT::OK)
         return OP_RESULT::OK;
      else if (lookupResult == OP_RESULT::LSM_DELETED)
         return OP_RESULT::NOT_FOUND;
   }

   return OP_RESULT::NOT_FOUND;
}

struct DTRegistry::DTMeta LSM::getMeta()
{
   DTRegistry::DTMeta lmsTree_meta = {.iterate_children = iterateChildrenSwips,
       .find_parent = findParent,
       .check_space_utilization = checkSpaceUtilization};
   return lmsTree_meta;
}

bool LSM::checkSpaceUtilization(void* btree_object, BufferFrame& bf, OptimisticGuard& o_guard, ParentSwipHandler& parent_handler)
{
   // TODO: implement, when xmerge should be used
   return false;
}

void LSM::iterateChildrenSwips(void*, BufferFrame& bufferFrame, std::function<bool(Swip<BufferFrame>&)> callback)
{
   auto& bTreeNode = *reinterpret_cast<btree::BTreeNode*>(bufferFrame.page.dt);
   if (bTreeNode.is_leaf) {
      return;
   }
   // bTreeNode is inner node -> has children
   for (u16 i = 0; i < bTreeNode.count; i++) {
      if (!callback(bTreeNode.getChild(i).cast<BufferFrame>())) {
         return;
      }
   }
   // process last child pointer (upper pointer)
   callback(bTreeNode.upper.cast<BufferFrame>());
}

struct ParentSwipHandler LSM::findParent(void* lsm_object, BufferFrame& to_find)
{
   // check on which level the bf to find is (page.dt is type btreeNode or BloomFilterPage
   // TODO: btreeNode needs to be of DataStructureIdentifier or we have to find the corresponding BTreeLL!
   DataStructureIdentifier* dsl = static_cast<DataStructureIdentifier*>(reinterpret_cast<DataStructureIdentifier*>(to_find.page.dt));
   u64 toFindLevel = dsl->level;
   LSM_TYPE toFindType = dsl->type;
   LSM* tree = static_cast<LSM*>(reinterpret_cast<LSM*>(lsm_object));

   tree->merge_mutex.lock();
   tree->merge_mutex.unlock();

   if (toFindType == LSM_TYPE::InMemoryBTree) {  // in-memory
      // in-Memory pages have set the keepInMemory flag
      jumpmu::jump();
   } else {  // on disk on level i
      // check which type bf is
      if (toFindType == LSM_TYPE::BTree) {  // BTree
         btree::BTreeLL* btreeLevel = &tree->tiers[toFindLevel]->tree;
         return btree::BTreeGeneric::findParent(*static_cast<btree::BTreeGeneric*>(reinterpret_cast<btree::BTreeLL*>(btreeLevel)), to_find);
      } else {  // BloomFilter
         //it might be a freed page as well (during merge); TODO: maybe increment version before reclaim in merge, because recheck in PageProviderThread.cpp:104 does not check if the page is FREE
         jumpmu::jump();
         BloomFilter* bf = &tree->tiers[toFindLevel]->filter;
         // TODO implement BloomFilter correct (findParent, etc)
         // return BloomFilter.findParent(*static_cast<BloomFilter*>(reinterpret_cast<BloomFilter*>(bf)), to_find);
      }
   }
}

}
}
}
