#include "lsm.hpp"
#include <cstdint>
#include <iostream>
#include <leanstore/storage/btree/core/BTreeGenericIterator.hpp>
#include <leanstore/storage/btree/core/BTreeNode.hpp>

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

struct BTreeIterator {
   // vector of BTreeNode and position of the current entry in this node (slotID)
   JMUW<vector<pair<btree::BTreeNode*, int>>> stack;

   BTreeIterator(btree::BTreeNode* node)
   {
      // If no further node or no entries in node, return
      if (!node || (node->is_leaf && node->count == 0))
         return;
      // add the node to the stack
      stack->push_back({node, -1});
      // move to the next entry
      ++(*this);
   }

   bool done() { return stack->empty(); }
   bool getDeletionFlag() { return stack->back().first->isDeleted(stack->back().second); }
   uint8_t* getKey() { return stack->back().first->getKey(stack->back().second); }
   unsigned getKeyLen() { return stack->back().first->slot[stack->back().second].key_len; }
   uint8_t* getPayload() { return stack->back().first->getPayload(stack->back().second); }
   unsigned getPayloadLen() { return stack->back().first->getPayloadLength(stack->back().second); }

   // returns the last BTreeNode in the stack
   btree::BTreeNode& operator*() const { return *stack.obj.back().first; }
   btree::BTreeNode* operator->() const { return &(operator*()); }

   // redefinition of operator ++ behaves like adding the next BTree nodes to the stack
   BTreeIterator& operator++()
   {
      while (!stack->empty()) {
         // get the last BTreeNode in the stack
         btree::BTreeNode* node = stack->back().first;
         // get the latest position in this BTreeNode
         int& pos = stack->back().second;
         if (node->is_leaf) {
            // leaf node
            if (pos + 1 < node->count) {
               // next entry in leaf
               pos++;
               return *this;
            }
            // remove leaf node when all node entries of him are processed
            stack->pop_back();
            // TODO from BTreeNodes to HybridPageGuards & if one node is done, do a page.reclaim directly
         } else {
            // inner node
            if (pos + 1 < node->count) {
               // down
               pos++;
               // add child at pos to the stack
               stack->push_back({(btree::BTreeNode*)node->getChild(pos).bf->page.dt, -1});
            } else if (pos + 1 == node->count) {
               // down (upper)
               pos++;
               // add last child with the highest values to the stack
               stack->push_back({(btree::BTreeNode*)node->upper.bf->page.dt, -1});
            } else {
               // up
               // TODO: all children are processed and in the stack, remove (inner parent) node?????
               stack->pop_back();
               // TODO: Do a page.reclaim for this inner node
            }
         }
      }
      return *this;
   }
};

unsigned spaceNeeded(unsigned prefix, unsigned totalKeySize, unsigned totalPayloadSize, unsigned count)
{
   // spaceNeeded = BTreeNodeHeader + slots * count + totalKeyAndPayloadSize + lowerFence + upperFence
   return sizeof(btree::BTreeNodeHeader) + (sizeof(btree::BTreeNode::Slot) * count) + totalKeySize - (count * prefix) + prefix + totalPayloadSize;
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

void buildNode(JMUW<vector<KeyValueEntry>>& entries, JMUW<vector<uint8_t>>& keyStorage, JMUW<vector<uint8_t>>& payloadStorage, btree::BTreeLL& btree)
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

            new_node->prefix_length = commonPrefix(entries->back().keyOffset + keyStorage->data(), entries->back().keyLen,
                                              entries->front().keyOffset + keyStorage->data(), entries->front().keyLen);
            for (unsigned i = 0; i < entries->size(); i++)
               new_node->storeKeyValueWithDeletionMarker(i,
                                  entries.obj[i].keyOffset + keyStorage->data(), entries.obj[i].keyLen,
                                  entries.obj[i].payloadOffset + payloadStorage->data(), entries.obj[i].payloadLen,
                                  entries.obj[i].deletionFlag);
            new_node->count = entries->size();
            new_node->insertFence(new_node->lower_fence, entries->back().keyOffset + keyStorage->data(),
                                  new_node->prefix_length);  // XXX: could put prefix before last key and set upperfence
            new_node->makeHint();

            btree.insertLeafNode(entries->back().keyOffset + keyStorage->data(), entries->back().keyLen, new_node);

            jumpmu_break;
      }
      jumpmuCatch() {}
   }
}

// adds the prefix and the key to keyStorage and returns the length of prefix and key
unsigned bufferKey(BTreeIterator& a, vector<uint8_t>& keyStorage)
{
   // insert the Prefix at the end of keyStorage
   unsigned pl = a->prefix_length;
   keyStorage.insert(keyStorage.end(), a->getLowerFenceKey(), a->getLowerFenceKey() + pl);

   // insert the key in the keyStorage
   unsigned kl = a.getKeyLen();
   keyStorage.insert(keyStorage.end(), a.getKey(), a.getKey() + kl);

   return pl + kl;
}

// adds the payload to payloadStorage and returns the length of the payload
unsigned bufferPayload(BTreeIterator& a, vector<uint8_t>& payloadStorage)
{
   // insert the key in the keyStorage
   unsigned pll = a.getPayloadLen();
   payloadStorage.insert(payloadStorage.end(), a.getPayload(), a.getPayload() + pll);

   return pll;
}

// adds the prefix, the key and the payload to keyStorage and returns the length of prefix, key and payload
unsigned bufferKeyValue(BTreeIterator& a, vector<uint8_t>& keyStorage, vector<uint8_t>& payloadStorage)
{
   // insert the Prefix at the end of keyStorage
   unsigned pl = a->prefix_length;
   keyStorage.insert(keyStorage.end(), a->getLowerFenceKey(), a->getLowerFenceKey() + pl);

   // insert the key in the keyStorage
   unsigned kl = a.getKeyLen();
   keyStorage.insert(keyStorage.end(), a.getKey(), a.getKey() + kl);

   // insert the key in the keyStorage
   unsigned pll = a.getPayloadLen();
   payloadStorage.insert(payloadStorage.end(), a.getPayload(), a.getPayload() + pll);

   return pl + kl + pll;
}

void LSM::createLeafNodeForSortedInsert(btree::BTreeNode* rootNode, DataStructureIdentifier* dsi) {
   while (true) {
      jumpmuTry()
      {
         // create empty leaf node linked to upper pointer to get insertLeafSorted working
         auto new_upper_node_h = HybridPageGuard<btree::BTreeNode>(dt_id);
         // upgrade to exclusive lock
         auto new_upper_node = ExclusivePageGuard<btree::BTreeNode>(std::move(new_upper_node_h));
         new_upper_node.init(true);
         new_upper_node->type = LSM_TYPE::BTree;
         new_upper_node->level = dsi->level;
         // connect the upper pointer of the empty root node to the new bufferFrame
         rootNode->upper = new_upper_node.getBufferFrame();

         jumpmu_break;
      }
      jumpmuCatch() {}
   }
}

// aTree is the root node (not the meta node!) of the first tree, bTree the root node of the second tree (may be null)
unique_ptr<StaticBTree> LSM::mergeTrees(HybridPageGuard<btree::BTreeNode>* aTreePG, HybridPageGuard<btree::BTreeNode>* bTreePG)
{
   auto newTree = make_unique<StaticBTree>();
   btree::BTreeNode* aTree = aTreePG->operator->();
   btree::BTreeNode* bTree = NULL;
   if (bTreePG != nullptr) {
      bTree = bTreePG->operator->();
   }

   // a new merged tree is always on disk, never a In-memory tree
   newTree->tree.type = LSM_TYPE::BTree;
   newTree->filter.type = LSM_TYPE::BloomFilter;

   //dsi needed for root bTreeNode
   DataStructureIdentifier dsi = DataStructureIdentifier();
   dsi.type = LSM_TYPE::BTree;

   if (bTree == NULL) { // we create the first level on disk, so the new merged tree has level 0 on disk
      newTree->tree.level = 0;
      newTree->filter.level = 0;
      dsi.level = 0;
   }
   else { // aTree is to large for level i, so its merged into bTree which resides at level i+1
      newTree->tree.level = bTree->level;
      newTree->filter.level = bTree->level;
      dsi.level = bTree->level;
   }

   // create new meta_node (could even pick the meta_node of aTree or bTree, but here we only know the root of these two trees, not the meta node)
   auto& newMetaNode = BMC::global_bf->allocatePage();
   Guard guard(newMetaNode.header.latch, GUARD_STATE::EXCLUSIVE);
   newMetaNode.header.keep_in_memory = true;
   newMetaNode.page.dt_id = this->dt_id;
   ((btree::BTreeNode*)newMetaNode.page.dt)->type = dsi.type;
   ((btree::BTreeNode*)newMetaNode.page.dt)->level = dsi.level;
   guard.unlock();

   newTree->tree.create(this->dt_id, &newMetaNode, &dsi);
   this->bTreeInMerge = &newTree->tree;

   btree::BTreeNode* metaNode = (btree::BTreeNode*)newTree->tree.meta_node_bf->page.dt;
   btree::BTreeNode* rootNode = (btree::BTreeNode*)metaNode->upper.bf->page.dt;
   rootNode->is_leaf = false;

   // we need a dummy node (upper ptr) get later the inner node in which we can insert complete nodes
   createLeafNodeForSortedInsert(rootNode, &dsi);

   uint64_t entryCount = 0;

   unsigned totalKeySize = 0;
   unsigned totalPayloadSize = 0;
   // saves the keyOffset and the length of the key and the payloadOffset and the length of the payload
   JMUW<vector<KeyValueEntry>> kvEntries;
   // saves the complete keys in one big list
   JMUW<vector<uint8_t>> keyStorage;
   // saves the payload in one big list
   JMUW<vector<uint8_t>> payloadStorage;
   keyStorage->reserve(btree::btreePageSize * 2);
   payloadStorage->reserve(btree::btreePageSize * 2);

   BTreeIterator a(aTree);
   BTreeIterator b(bTree);
   JMUW<vector<uint8_t>> keyA, keyB;
   keyA->reserve(btree::btreePageSize);
   keyB->reserve(btree::btreePageSize);

   JMUW<vector<uint64_t>> hashes;

   while (true) {
      BTreeIterator* it;

      if (a.done()) {
         if (b.done()) {
            // all entries processed, create last page and exit
            entryCount += kvEntries->size();
            buildNode(kvEntries, keyStorage, payloadStorage, newTree->tree);

            // create BloomFilter
            /*newTree->filter.init(entryCount);
            for (auto h : hashes)
               newTree->filter.insert(h);*/

            // release all nodes of the two merged trees
            if (aTree->type == LSM_TYPE::InMemoryBTree) {
               //merge between InMemBTree and tier[0]
               int beforeCounter = this->inMemBTree->countPages();
               int counter = this->inMemBTree->releaseAllPagesRec(*aTreePG);
               assert(counter == beforeCounter);

               //bTree can be null, when only the inMemTree exists
               if (bTree != NULL) {
                  beforeCounter = this->tiers[0]->tree.countPages();
                  counter = this->tiers[0]->tree.releaseAllPagesRec(*bTreePG);
                  assert(counter == beforeCounter);
               }
            }
            else {
               //merge between tier[x] and tier[x+1]
               int beforeCounter = this->tiers[dsi.level-1]->tree.countPages();
               int counter = this->tiers[dsi.level-1]->tree.releaseAllPagesRec(*aTreePG);
               assert(counter == beforeCounter);

               //bTree cant be null, because when the tree at level x is too large it is only moved to level x+1 in mergeAll()
               if (bTree != NULL) {
                  beforeCounter = this->tiers[dsi.level]->tree.countPages();
                  counter = this->tiers[dsi.level]->tree.releaseAllPagesRec(*bTreePG);
                  assert(counter == beforeCounter);
               }
            }

            return newTree;
         }
         it = &b;
      } else if (b.done()) {
         it = &a;
      } else {  // take smaller element
         keyA->resize(0);
         bufferKey(a, keyA.obj);
         keyB->resize(0);
         bufferKey(b, keyB.obj);
         int compareValue = cmpKeys(keyA->data(), keyB->data(), keyA->size(), keyB->size());
         if (compareValue == 0) {
            //both keys have the same key; this can happen through an update or an deletion (or a duplicate insert!!!!)
            // so TODO: avoid inserting duplicate keys
            //keyA is the newer key (higher LSM tree tier)
            if (a.getDeletionFlag()) {
               // key was in fact deleted, but we need the deletion entry for further levels where the original key may occur again!
               // only if bTree is the lowest tier of the lsm tree we can forget the deletion marker and delete the entry really :)
               if (bTree->level == this->tiers.size()-1) {
                  ++a;
               }
               //could also test the bloom filters of further levels or do a lookup if the key occurs in a lower level, then we could delete the entry as well
            }
            // b is in every case the older value, isnt needed anymore (regardless if its a deleted entry, an updated entry or an originally inserted entry)
            ++b;
            continue;
         }
         else {
            it = compareValue < 0 ? &a : &b;
         }
      }
      unsigned keyOffset = keyStorage->end() - keyStorage->begin();
      unsigned keyLen = bufferKey(*it, keyStorage.obj);
      unsigned payloadOffset = payloadStorage->end() - payloadStorage->begin();
      unsigned payloadLen = bufferPayload(*it, payloadStorage.obj);
      bool deletionFlag = it->getDeletionFlag();

      kvEntries->push_back({keyOffset, keyLen, payloadOffset, payloadLen, deletionFlag});

      uint8_t* key = keyStorage->data() + keyStorage->size() - keyLen;
      //uint8_t* payload = payloadStorage.data() + payloadStorage.size() - payloadLen;
      hashes->push_back(BloomFilter::hashKey(key, keyLen));

      unsigned prefix = commonPrefix(kvEntries->front().keyOffset + keyStorage->data(), kvEntries->front().keyLen, key, keyLen);
      if (spaceNeeded(prefix, totalKeySize + keyLen, totalPayloadSize + payloadLen, kvEntries->size()) >= btree::btreePageSize) {
         // key does not fit, create new page
         kvEntries->pop_back();
         entryCount += kvEntries->size();
         buildNode(kvEntries, keyStorage, payloadStorage, newTree->tree);  // could pick shorter separator here
         kvEntries->clear();
         keyStorage->clear();
         payloadStorage->clear();
         totalKeySize = 0;
         totalPayloadSize = 0;
      } else {
         // key fits, buffer it
         // go to next entry in the tree
         ++(*it);
         totalKeySize += keyLen;
         totalPayloadSize += payloadLen;
      }
   }
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

   // set the dsi for the InMemBTree of the LSM-Tree
   this->inMemBTree->type = dsi.type;
   this->inMemBTree->level = dsi.level;
}

void LSM::printLevels()
{
   uint64_t sum = 0;
   for (auto& t : tiers) {
      sum += t->tree.countPages();
      cout << t->tree.countPages() << " ";
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
      if (curr.tree.countPages() >= limit) {
         // current observed BTree is too large
         this->levelInMerge = i+1;
         if (i + 1 < tiers.size()) {
            // merge with next level
            StaticBTree& next = *tiers[i + 1];

            HybridPageGuard<btree::BTreeNode> currMetaNode_guard(curr.tree.meta_node_bf);
            HybridPageGuard<btree::BTreeNode> nextMetaNode_guard(next.tree.meta_node_bf);
            HybridPageGuard<btree::BTreeNode> currRootPageGuard = HybridPageGuard<btree::BTreeNode>(currMetaNode_guard, currMetaNode_guard->upper);
            HybridPageGuard<btree::BTreeNode> nextRootPageGuard = HybridPageGuard<btree::BTreeNode>(nextMetaNode_guard, nextMetaNode_guard->upper);

            tiers[i + 1] = mergeTrees(&currRootPageGuard, &nextRootPageGuard);

            //release meta nodes after merge
            ExclusivePageGuard<btree::BTreeNode> currMetaNode_x_guard(std::move(currMetaNode_guard));
            ExclusivePageGuard<btree::BTreeNode> nextMetaNode_x_guard(std::move(nextMetaNode_guard));
            currMetaNode_x_guard.reclaim();
            nextMetaNode_x_guard.reclaim();

            ensure(tiers[i+1]->tree.type == LSM_TYPE::BTree);
            ensure(tiers[i+1]->filter.type == LSM_TYPE::BloomFilter);
            ensure(tiers[i+1]->tree.level == i+1);
            ensure(tiers[i+1]->filter.level == i+1);
         } else {
            // new level
            cout << "neues level, jetzt: " << i+1 << " DTID: " << this->dt_id << " countEntries LSM: " << this->countEntries() <<endl;
            tiers.emplace_back(move(tiers.back()));
            tiers[i+1]->tree.type = LSM_TYPE::BTree;
            tiers[i+1]->tree.level = i+1;
            tiers[i+1]->filter.type = LSM_TYPE::BloomFilter;
            tiers[i+1]->filter.level = i+1;
            ((btree::BTreeNode*)tiers[i+1]->tree.meta_node_bf->page.dt)->type = LSM_TYPE::BTree;
            ((btree::BTreeNode*)tiers[i+1]->tree.meta_node_bf->page.dt)->level = i+1;

            // we need to set every BTreeNode->level to the new level i+1 since no mergeTrees was issued
            tiers[i+1]->tree.iterateAllPages([i](btree::BTreeNode& innerNode) { innerNode.level = i+1; return 0; }, [i](btree::BTreeNode& leafNode) { leafNode.level = i+1; return 0; });
         }

         tiers[i] = make_unique<StaticBTree>();

         DataStructureIdentifier dsi = DataStructureIdentifier();
         dsi.type = LSM_TYPE::BTree;
         dsi.level = i;

         // create new metaPage for the tiers[i] BTree
         auto& newMetaBufferPage = BMC::global_bf->allocatePage();
         Guard guard(newMetaBufferPage.header.latch, GUARD_STATE::EXCLUSIVE);
         newMetaBufferPage.header.keep_in_memory = true;
         newMetaBufferPage.page.dt_id = this->dt_id;
         ((btree::BTreeNode*)newMetaBufferPage.page.dt)->type = dsi.type;
         ((btree::BTreeNode*)newMetaBufferPage.page.dt)->level = dsi.level;
         guard.unlock();

         //create btree for level tiers[i]
         tiers[i]->tree.create(this->dt_id, &newMetaBufferPage, &dsi);
         tiers[i]->tree.type = dsi.type;
         tiers[i]->tree.level = dsi.level;
         tiers[i]->filter.type = LSM_TYPE::BloomFilter;
         tiers[i]->filter.level = i;
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


OP_RESULT LSM::updateSameSize(u8* key, u16 keyLength, function<void(u8* payload, u16 payloadSize)> callback)
{
   u8* keyPayload;
   u16 keyPayloadLength;

   // TODO: maybe a new updateSameSize in adapter?, because with a callback function we need to find the the previous payload first OR
   // Workaround: save the key, keylength, and changed values (with position and length) and resolve it during merge and during scan/select (difficult!)

   // case1: record to update is already in the inMemTree
   //      case1.1: record is marked as deleted -> NOT FOUND
   //      case1.2: normal record (because of insert or earlier update) -> updateSameSize
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
      return this->inMemBTree->updateSameSize(key, keyLength, callback);
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
         return LSM::insert(key, keyLength, keyPayload, keyPayloadLength);
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
      if (this->lookup(key, keyLength, [&](const u8* payload, u16 payload_length){}) == OP_RESULT::OK) {
         // occurs in one level below and is not deleted yet
         return insertWithDeletionMarker(key, keyLength, nullptr, NULL, true);
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

OP_RESULT LSM::scanAsc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
{
   //TODO: Scan all trees (parallel?) and take the smallest Element and if more versions across the levels exist take the newest

   Slice key(start_key, key_length);
   jumpmuTry()
   {
      btree::BTreeSharedIterator iterator(*static_cast<btree::BTreeLL*>(inMemBTree.get()));
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

OP_RESULT LSM::scanDesc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
{
   //TODO: Scan all trees (parallel?) and take the biggest Element and if more than one version exists across the levels, take the newest

   Slice key(start_key, key_length);
   jumpmuTry()
   {
      btree::BTreeSharedIterator iterator(*static_cast<btree::BTreeLL*>(inMemBTree.get()));
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

// returns the number of pages in the LSM Tree
u64 LSM::countPages()
{
   u64 pageCount = inMemBTree->countPages();

   for (unsigned i = 0; i < tiers.size(); i++) {
      pageCount += tiers[i]->tree.countPages();
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
   // case1: key does not occur in the hole LSM Tree -> normal insert
   // case2: key occurs in the inMemBtree
   //      case2.1: key is a deletion entry -> duplicate key with normal insert, but should be ok (delete deleted entry + insert new entry -> in the end an "update", but maybe with different payload length)
   //      case2.2: non-deleted entry -> insert fails with duplicate key
   // case3: key occurs in a lower tier -> insert would be ok!! So check the further levels before if key occurs
   //      case3.1: key occurs in a next lower tier and not marked as deleted -> dont allow insert
   //      case3.2: key occurs in a next lower tier, but deletion marker is set in the first/newest lower tier -> could insert value again
   //            -> lower levels with the same key may exist but arent of interest (are deleted during merge)

   // first lookup inMemTree
   OP_RESULT lookupResult = inMemBTree->lookup(key, keyLength, [&](const u8*, u16){});
   if (lookupResult == OP_RESULT::OK) {
      // case2.2
      return OP_RESULT::DUPLICATE;
   } else if (lookupResult == OP_RESULT::LSM_DELETED) {
      // case2.1
      // real delete + insert
      inMemBTree->remove(key, keyLength);
   } else {
      // lookup tiers
      // optional: parallel lookup with queue
      for (unsigned i = 0; i < tiers.size(); i++) {
         // TODO enable filter lookup again
         // if (tiers[i]->filter.lookup(key, keyLength) && tiers[i]->tree.lookup(key, keyLength,payload_callback) == OP_RESULT::OK)
         lookupResult = tiers[i]->tree.lookup(key, keyLength, [&](const u8*, u16){});
         if (lookupResult == OP_RESULT::OK) {
            // case3.1
            return OP_RESULT::DUPLICATE;
         } else if (lookupResult == OP_RESULT::LSM_DELETED) {
            // case3.2
            break;
         }
      }
   }
   //case1 + case3.2 + case2.1



   auto result = inMemBTree->insertWithDeletionMarker(key, keyLength, payload, payloadLength, deletionMarker);
   u64 pageCount = inMemBTree->countPages();
   if (pageCount >= baseLimit) {
      // merge inMemory-BTree with first level BTree of LSM Tree
      this->inMerge = true;
      this->levelInMerge = 0;

      // init pageGuard to the metaNode
      HybridPageGuard<btree::BTreeNode> p_guard(inMemBTree->meta_node_bf);
      // p_guard->upper is the root node, rootPageGuard.bufferFrame is set to the root node
      HybridPageGuard<btree::BTreeNode> rootPageGuard = HybridPageGuard<btree::BTreeNode>(p_guard, p_guard->upper);

      // get the root node of the btree of level 0, if the level exists
      btree::BTreeNode* old = tiers.size() ? (btree::BTreeNode*)((btree::BTreeNode*)tiers[0]->tree.meta_node_bf->page.dt)->upper.bf->page.dt : nullptr;

      // merge the two trees / pull the in-mem-tree down (if only in-mem-tree exists)
      unique_ptr<StaticBTree> neu;
      if (old != nullptr) {
         this->bTreeInMerge = &tiers[0]->tree;
         HybridPageGuard<btree::BTreeNode> old_guard(tiers[0]->tree.meta_node_bf);
         HybridPageGuard<btree::BTreeNode> old_RootPageGuard = HybridPageGuard<btree::BTreeNode>(old_guard, old_guard->upper);
         tiers[0] = mergeTrees(&rootPageGuard, &old_RootPageGuard);

         //release meta nodes after merge
         ExclusivePageGuard<btree::BTreeNode> inMemMetaNode_x_guard(std::move(p_guard));
         ExclusivePageGuard<btree::BTreeNode> tier0MetaNode_x_guard(std::move(old_guard));
         inMemMetaNode_x_guard.reclaim();
         tier0MetaNode_x_guard.reclaim();
      }
      else {
         tiers.emplace_back(move(mergeTrees(&rootPageGuard, nullptr)));

         //release the meta node after merge
         ExclusivePageGuard<btree::BTreeNode> inMemMetaNode_x_guard(std::move(p_guard));
         inMemMetaNode_x_guard.reclaim();
      }
      //cout << "mergeTrees() in insert, lower fence von neu: "<< ((btree::BTreeNode*)neu->tree.meta_node_bf->page.dt)->getLowerFenceKey() << " upper fence: "<< ((btree::BTreeNode*)neu->tree.meta_node_bf->page.dt)->getUpperFenceKey() << endl;

      ensure(tiers[0]->tree.type == LSM_TYPE::BTree);
      ensure(tiers[0]->filter.type == LSM_TYPE::BloomFilter);
      ensure(tiers[0]->tree.level == 0);
      ensure(tiers[0]->filter.level == 0);

      // generate new empty inMemory BTree
      this->create(this->dt_id, this->meta_node_bf);

      // if necessary merge further levels
      mergeAll();
      this->inMerge = false;
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

   //cout << endl << "lookup key = " << key << endl;

   OP_RESULT lookupResult = inMemBTree->lookup(key, keyLength, payload_callback);
   if (lookupResult == OP_RESULT::OK)
      return OP_RESULT::OK;
   else if (lookupResult == OP_RESULT::LSM_DELETED)
      return OP_RESULT::NOT_FOUND;

   // optional: parallel lookup with queue
   for (unsigned i = 0; i < tiers.size(); i++) {
      // TODO enable filter lookup again
      //if (tiers[i]->filter.lookup(key, keyLength) && tiers[i]->tree.lookup(key, keyLength,payload_callback) == OP_RESULT::OK)
      lookupResult = tiers[i]->tree.lookup(key, keyLength,payload_callback);
      if (lookupResult == OP_RESULT::OK)
         return OP_RESULT::OK;
      else if (lookupResult == OP_RESULT::LSM_DELETED)
         return OP_RESULT::NOT_FOUND;
   }

   return OP_RESULT::NOT_FOUND;
}

struct DataTypeRegistry::DTMeta LSM::getMeta()
{
   DataTypeRegistry::DTMeta lmsTree_meta = {.iterate_children = iterateChildrenSwips,
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

static int counterInMerge=0;
struct ParentSwipHandler LSM::findParent(void* lsm_object, BufferFrame& to_find)
{
   // check on which level the bufferFrame to find is (page.dt is type btreeNode or BloomFilterPage
   // TODO: btreeNode needs to be of DataStructureIdentifier or we have to find the corresponding BTreeLL!
   DataStructureIdentifier* dsl = static_cast<DataStructureIdentifier*>(reinterpret_cast<DataStructureIdentifier*>(to_find.page.dt));
   u64 toFindLevel = dsl->level;
   LSM_TYPE toFindType = dsl->type;
   LSM* tree = static_cast<LSM*>(reinterpret_cast<LSM*>(lsm_object));

   // check, if merge between two level is ongoing
   if (tree->inMerge && tree->levelInMerge == toFindLevel) {//toFindLevel == tree->bTreeInMerge->level) {
      counterInMerge++;
      cout << "Could not find parent counter: " << counterInMerge << endl;
      jumpmu::jump();
      if (toFindType != LSM_TYPE::InMemoryBTree && toFindLevel == tree->bTreeInMerge->level) {
         // page to_find can be in old btree at toFindLevel or can be the new created btree
         // TODO check old btree and btree in built
         jumpmuTry() {
            btree::BTreeLL* btreeLevel = &(tree->tiers[toFindLevel]->tree);
            return btree::BTreeGeneric::findParent(*static_cast<btree::BTreeGeneric*>(reinterpret_cast<btree::BTreeLL*>(btreeLevel)), to_find);
         }
         jumpmuCatch(){// Not found in the level, Buffer Manager should retry and find another buffer frame
            jumpmu::jump();
            //could also check the currently in build tree:
            //btree::BTreeLL* btreeLevel = tree->bTreeInMerge;
            //return btree::BTreeGeneric::findParent(*static_cast<btree::BTreeGeneric*>(reinterpret_cast<btree::BTreeLL*>(btreeLevel)), to_find);
         }
      }
      // else page to_find not in one of the merged trees (can be in the old InMemTree, or in levels which arent merged, ...)
      // so continue normal
   }

   if (toFindType == LSM_TYPE::InMemoryBTree) { // in-memory
      btree::BTreeLL* btreeRootLSM = tree->inMemBTree.get();
      return btree::BTreeGeneric::findParent(*static_cast<btree::BTreeGeneric*>(reinterpret_cast<btree::BTreeLL*>(btreeRootLSM)), to_find);
   }
   else { // on disk on level i
      // check which type bufferFrame is
      if (toFindType == LSM_TYPE::BTree) { //BTree
         btree::BTreeLL* btreeLevel = &(tree->tiers[toFindLevel]->tree);
         return btree::BTreeGeneric::findParent(*static_cast<btree::BTreeGeneric*>(reinterpret_cast<btree::BTreeLL*>(btreeLevel)), to_find);
      }
      else { // BloomFilter
         BloomFilter* bf = &(tree->tiers[toFindLevel]->filter);
         // TODO implement BloomFilter correct (findParent, etc)
         //return BloomFilter.findParent(*static_cast<BloomFilter*>(reinterpret_cast<BloomFilter*>(bf)), to_find);
      }
   }
}

}
}
}
