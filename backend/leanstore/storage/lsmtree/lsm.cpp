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
struct KeyEntry {
   unsigned keyOffset;
   unsigned len;
};

struct KeyValueEntry {
   unsigned keyOffset;
   unsigned keyLen;
   unsigned payloadOffset;
   unsigned payloadLen;
};

// Compare two string
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
            }
         }
      }
      return *this;
   }
};

unsigned spaceNeeded(unsigned prefix, unsigned totalKeySize, unsigned totalPayloadSize, unsigned count)
{
   // spaceNeeded = BTreeNodeHeader + slots * count + totalKeyAndPayloadSize + lowerFenxe + upperFence
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

void buildNode(vector<KeyValueEntry>& entries, vector<uint8_t>& keyStorage, vector<uint8_t>& payloadStorage, btree::BTreeLL& btree)
{
   auto new_node_h = HybridPageGuard<btree::BTreeNode>(btree.dt_id);
   auto new_node = ExclusivePageGuard<btree::BTreeNode>(std::move(new_node_h));
   new_node.init(true);

   // Workaround, set keep_in_memory true to avoid swap to disk, because LSM-Level Tree is not set yet
   new_node.getBufferFrame()->header.keep_in_memory = true;

   new_node->type = LSM_TYPE::BTree;
   new_node->level = btree.level;

   new_node->prefix_length = commonPrefix(entries.back().keyOffset + keyStorage.data(), entries.back().keyLen,
                                     entries.front().keyOffset + keyStorage.data(), entries.front().keyLen);
   for (unsigned i = 0; i < entries.size(); i++)
      new_node->storeKeyValue(i,
                         entries[i].keyOffset + keyStorage.data(), entries[i].keyLen,
                         entries[i].payloadOffset + payloadStorage.data(), entries[i].payloadLen);
   new_node->count = entries.size();
   new_node->insertFence(new_node->lower_fence, entries.back().keyOffset + keyStorage.data(),
                         new_node->prefix_length);  // XXX: could put prefix before last key and set upperfence
   new_node->makeHint();

   btree.insertLeafNode(entries.back().keyOffset + keyStorage.data(), entries.back().keyLen, new_node);
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

   // create empty leaf node linked to upper pointer to get insertLeafSorted working
   auto new_upper_node_h = HybridPageGuard<btree::BTreeNode>(dt_id);
   // upgrade to exclusive lock
   auto new_upper_node = ExclusivePageGuard<btree::BTreeNode>(std::move(new_upper_node_h));
   new_upper_node.init(true);
   new_upper_node->type = LSM_TYPE::BTree;
   new_upper_node->level = dsi->level;
   // connect the upper pointer of the empty root node to the new bufferFrame
   rootNode->upper = new_upper_node.getBufferFrame();

   // Workaround, set keep_in_memory true to avoid swap to disk, because LSM-Level Tree is not set yet
   new_upper_node.getBufferFrame()->header.keep_in_memory=true;
}

// aTree is the root node (not the meta node!) of the first tree, bTree the root node of the second tree (may be null)
unique_ptr<StaticBTree> LSM::mergeTrees(btree::BTreeNode* aTree, btree::BTreeNode* bTree)
{
   auto newTree = make_unique<StaticBTree>();

 //  auto newTreeLL = btree::BTreeLL();

/*
   //create new btree
   DTID dtid = DataTypeRegistry::global_dt_registry.registerDatastructureInstance(1, reinterpret_cast<void*>(&newTree->tree), aTree);
   auto& bf = buffer_manager->allocatePage();
   Guard guard(bf.header.latch, GUARD_STATE::EXCLUSIVE);
   bf.header.keep_in_memory = true;
   bf.page.dt_id = dtid;
   guard.unlock();
   btree.create(dtid, &bf);


   newTree->tree.(0);
*/


   // a new merged tree is always in disk, never a In-memory tree
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
   else { // aTree is to large for level i, so its merged into bTree which reside at level i+1
      newTree->tree.level = bTree->level;
      newTree->filter.level = bTree->level;
      dsi.level = bTree->level;
   }

   // create new meta_node (could even pick the meta_node of aTree or bTree, but here we only know the root of these two trees, not the meta node)
   auto& newMetaNode = BMC::global_bf->allocatePage();
   Guard guard(newMetaNode.header.latch, GUARD_STATE::EXCLUSIVE);
   newMetaNode.header.keep_in_memory = true;
   newMetaNode.page.dt_id = this->dt_id;
   guard.unlock();

   newTree->tree.create(this->dt_id, &newMetaNode, &dsi);
   btree::BTreeNode* metaNode = (btree::BTreeNode*)newTree->tree.meta_node_bf->page.dt;
   btree::BTreeNode* rootNode = (btree::BTreeNode*)metaNode->upper.bf->page.dt;
   rootNode->is_leaf = false;

   // Workaround, set keep_in_memory true to avoid swap to disk, because LSM-Level Tree is not set yet
   metaNode->upper.bf->header.keep_in_memory=true;

   // create new metaPage for the In-memory BTree
   /*
   auto& newMetaBufferPage = BMC::global_bf->allocatePage();
   Guard guard(newMetaBufferPage.header.latch, GUARD_STATE::EXCLUSIVE);
   newMetaBufferPage.header.keep_in_memory = true;
   newMetaBufferPage.page.dt_id = dtid;
   guard.unlock();
   */

   createLeafNodeForSortedInsert(rootNode, &dsi);


   //newTree->tree.pageCount++;
   uint64_t entryCount = 0;

   unsigned totalKeySize = 0;
   unsigned totalPayloadSize = 0;
   // saves the keyOffset and the length of the key and the payloadOffset and the length of the payload
   vector<KeyValueEntry> kvEntries;
   // saves the complete keys in one big list
   vector<uint8_t> keyStorage;
   // saves the payload in one big list
   vector<uint8_t> payloadStorage;
   keyStorage.reserve(btree::btreePageSize * 2);
   payloadStorage.reserve(btree::btreePageSize * 2);

   BTreeIterator a(aTree);
   BTreeIterator b(bTree);
   vector<uint8_t> keyA, keyB;
   keyA.reserve(btree::btreePageSize);
   keyB.reserve(btree::btreePageSize);

   vector<uint64_t> hashes;

   while (true) {
      BTreeIterator* it;

      if (a.done()) {
         if (b.done()) {
            // all entries processed, create last page and exit
            entryCount += kvEntries.size();
            buildNode(kvEntries, keyStorage, payloadStorage, newTree->tree);

            // create BloomFilter
            newTree->filter.init(entryCount);
            for (auto h : hashes)
               newTree->filter.insert(h);

            return newTree;
         }
         it = &b;
      } else if (b.done()) {
         it = &a;
      } else {  // take smaller element
         keyA.resize(0);
         bufferKey(a, keyA);
         keyB.resize(0);
         bufferKey(b, keyB);
         it = cmpKeys(keyA.data(), keyB.data(), keyA.size(), keyB.size()) < 0 ? &a : &b;
      }
      unsigned keyOffset = keyStorage.end() - keyStorage.begin();
      unsigned keyLen = bufferKey(*it, keyStorage);
      unsigned payloadOffset = payloadStorage.end() - payloadStorage.begin();
      unsigned payloadLen = bufferPayload(*it, payloadStorage);

      kvEntries.push_back({keyOffset, keyLen, payloadOffset, payloadLen});

      uint8_t* key = keyStorage.data() + keyStorage.size() - keyLen;
      //uint8_t* payload = payloadStorage.data() + payloadStorage.size() - payloadLen;
      hashes.push_back(BloomFilter::hashKey(key, keyLen));

      unsigned prefix = commonPrefix(kvEntries.front().keyOffset + keyStorage.data(), kvEntries.front().keyLen, key, keyLen);
      if (spaceNeeded(prefix, totalKeySize + keyLen, totalPayloadSize + payloadLen, kvEntries.size()) >= btree::btreePageSize) {
         // key does not fit, create new page
         kvEntries.pop_back();
         entryCount += kvEntries.size();
         buildNode(kvEntries, keyStorage, payloadStorage, newTree->tree);  // could pick shorter separator here
         kvEntries.clear();
         keyStorage.clear();
         payloadStorage.clear();
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

   // create new metaPage for the In-memory BTree
   auto& newMetaBufferPage = BMC::global_bf->allocatePage();
   Guard guard(newMetaBufferPage.header.latch, GUARD_STATE::EXCLUSIVE);
   newMetaBufferPage.header.keep_in_memory = true;
   newMetaBufferPage.page.dt_id = dtid;
   guard.unlock();

   // create the inMemory BTree
   // TODO use of dtid for more than one BTree is valid? dtid = dtid of LSM tree

   DataStructureIdentifier dsi = DataStructureIdentifier();
   dsi.type = LSM_TYPE::InMemoryBTree;
   dsi.level = 0;

   this->inMemBTree->create(dtid, &newMetaBufferPage, &dsi);

   this->inMemBTree->type=LSM_TYPE::InMemoryBTree;
   this->inMemBTree->level = 0;

/*
   // Allocate a first page of the LSM Tree (for the root node)
   auto root_write_guard_h = HybridPageGuard<LSM>(dtid);
   // set the page exclusive to alter/init information
   auto root_write_guard = ExclusivePageGuard<LSM>(std::move(root_write_guard_h));
   // after exclusive access init the root page of the LSM tree
   root_write_guard.init();

   // -------------------------------------------------------------------------------------
   // Set the meta node for this LSM Tree
   this->meta_node_bf = meta_bf;
   // Set the datastructure id for the LSM Tree (counting number for all datastructures in LeanStore
   this->dt_id = dtid;

   // create the in-memory BTree (first of all one root node)
   // therefore it should be with bf.header.keep_in_memory=true for all Nodes of the in-mem BTree
   // in merge and mergeAll the level-trees have to be keep_in_memory=false!
   // datastructure ID for inMemBTree is 0, first level has ID=1, ...
   auto newBTreeNodeHybrid = HybridPageGuard<btree::BTreeNode>(dt_id, false);
   auto newRootExclusive = ExclusivePageGuard<btree::BTreeNode>(std::move(newBTreeNodeHybrid));

   DTID inMemBTreeDtid = 0;
   auto& bf = buffer_manager->allocatePage();
   Guard guard(bf.header.latch, GUARD_STATE::EXCLUSIVE);
   bf.header.keep_in_memory = true;
   bf.page.dt_id = dtid;
   guard.unlock();
   btree.create(dtid, &bf);

   // why first HybridPageGuard and then Exclusive; why not directly ExclusivePageGuard?
   HybridPageGuard<LSM> meta_guard(meta_bf);
   // exclusive locking for the meta node
   ExclusivePageGuard meta_page(std::move(meta_guard));*/
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
         if (i + 1 < tiers.size()) {
            // merge with next level
            StaticBTree& next = *tiers[i + 1];
            btree::BTreeNode* currMetaNode = (btree::BTreeNode*)curr.tree.meta_node_bf->page.dt;
            btree::BTreeNode* nextMetaNode = (btree::BTreeNode*)next.tree.meta_node_bf->page.dt;
            tiers[i + 1] = mergeTrees((btree::BTreeNode*)currMetaNode->upper.bfPtr()->page.dt, (btree::BTreeNode*)nextMetaNode->upper.bfPtr()->page.dt);

            // new layer is ready, set all pages to keepInMemory=false
            tiers[i+1]->tree.iterateAllPagesNodeGuard([](HybridPageGuard<btree::BTreeNode>& innerNode) { innerNode.bufferFrame->header.keep_in_memory=false; return 0; },
                                               [](HybridPageGuard<btree::BTreeNode>& leafNode) { leafNode.bufferFrame->header.keep_in_memory=false; return 0; });
            // set every BTreeNode->level to the new level i+1
            tiers[i+1]->tree.iterateAllPages([i](btree::BTreeNode& innerNode) { innerNode.level = i+1; return 0; }, [i](btree::BTreeNode& leafNode) { leafNode.level = i+1; return 0; });

            //nicht mehr nötig, da bereits in mergeTrees gesetzt
            /*
            tiers[i+1]->tree.type = LSM_TYPE::BTree;
            tiers[i+1]->tree.level = i+1;
            tiers[i+1]->filter.type = LSM_TYPE::BloomFilter;
            tiers[i+1]->filter.level = i+1;
             */
            ensure(tiers[i+1]->tree.type == LSM_TYPE::BTree);
            ensure(tiers[i+1]->filter.type == LSM_TYPE::BloomFilter);
            ensure(tiers[i+1]->tree.level == i+1);
            ensure(tiers[i+1]->filter.level == i+1);
         } else {
            // new level
            tiers.emplace_back(move(tiers.back()));
            tiers[i+1]->tree.type = LSM_TYPE::BTree;
            tiers[i+1]->tree.level = i+1;
            tiers[i+1]->filter.type = LSM_TYPE::BloomFilter;
            tiers[i+1]->filter.level = i+1;
            // set every BTreeNode->level to the new level i+1
            tiers[i+1]->tree.iterateAllPages([i](btree::BTreeNode& innerNode) { innerNode.level = i+1; return 0; }, [i](btree::BTreeNode& leafNode) { leafNode.level = i+1; return 0; });
         }

         tiers[i] = make_unique<StaticBTree>();

         // create new metaPage for the tiers[i] BTree
         auto& newMetaBufferPage = BMC::global_bf->allocatePage();
         Guard guard(newMetaBufferPage.header.latch, GUARD_STATE::EXCLUSIVE);
         newMetaBufferPage.header.keep_in_memory = true;
         newMetaBufferPage.page.dt_id = this->dt_id;
         guard.unlock();

         //create btree for level tiers[i]
         tiers[i]->tree.create(this->dt_id, &newMetaBufferPage);
         tiers[i]->tree.type = LSM_TYPE::BTree;
         tiers[i]->tree.level = i;
         tiers[i]->filter.type = LSM_TYPE::BloomFilter;
         tiers[i]->filter.level = i;
      }
      limit = limit * factor;
   }
}
/*
void LSM::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   inMemBTree->insert(key, keyLength, payload, payloadLength);


   HybridPageGuard<btree::BTreeNode> p_guard(inMemBTree->meta_node_bf);
   // p_guard->upper is root, target_guard set to the root node
   HybridPageGuard<btree::BTreeNode> root = HybridPageGuard<btree::BTreeNode>(p_guard, p_guard->upper);

   if (inMemBTree->countPages() >= baseLimit) {
      // merge inMemory-BTree with first level BTree of LSM Tree
      btree::BTreeNode* old = tiers.size() ? (btree::BTreeNode*)tiers[0]->tree.meta_node_bf->page.dt : nullptr;
      unique_ptr<StaticBTree> neu = mergeTrees((btree::BTreeNode*)root.bufferFrame->page.dt, old);
      if (tiers.size())
         tiers[0] = move(neu);
      else
         tiers.emplace_back(move(neu));
      // generate new empty inMemory BTree
      inMemBTree = make_unique<btree::BTreeLL>();
      inMemBTree->create(this->dt_id, this->meta_node_bf);

      // if necessary merge further levels
      mergeAll();
   }
}*/
/*
// returns true when the key is found in the inMemory BTree (Root of LSM-Tree)
bool LSM::lookup(uint8_t* key, unsigned keyLength)
{
   if (inMemBTree->lookup(key, keyLength))
      return true;

   for (unsigned i = 0; i < tiers.size(); i++) {
      if (tiers[i]->filter.lookup(key, keyLength) && tiers[i]->tree.lookup(key, keyLength))
         return true;
   }

   return false;
}

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

// for LeanStore Adapter
// searches for the
OP_RESULT LSM::updateSameSize(u8* key, u16 keyLength, function<void(u8* value, u16 value_size)> callback)
{
   u8 keyPayload;
   u16 keyPayloadLength;

   LSM::lookup(key, keyLength,[&](const u8* payload, u16 payload_length) {
     static_cast<void>(payload_length);
     u8& typed_payload = *const_cast<u8*>(reinterpret_cast<const u8*>(payload));
     keyPayload = typed_payload;
     keyPayloadLength = payload_length;
   });
   LSM::insert(key, keyLength, &keyPayload, keyPayloadLength);
}

OP_RESULT LSM::remove(u8* key, u16 key_length)
{
   return insert(key, key_length,NULL,NULL);
   //TODO: Add DELETE-entry in LSM Tree and remove the entry during merge
   // is NULL safe as Deletion marker?
}

OP_RESULT LSM::scanAsc(u8* start_key, u16 key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
{
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

// inserts a value in the In-memory Level Tree of the LSM tree and merges with lower levels if necessary
OP_RESULT LSM::insert(u8* key, u16 keyLength, u8* payload, u16 payloadLength)
{
      auto result = inMemBTree->insert(key, keyLength, payload, payloadLength);
      u64 pageCount = inMemBTree->countPages();
      if (pageCount >= baseLimit) {
         // merge inMemory-BTree with first level BTree of LSM Tree

         // init pageGuard to the metaNode
         HybridPageGuard<btree::BTreeNode> p_guard(inMemBTree->meta_node_bf);
         // p_guard->upper is the root node, rootPageGuard.bufferFrame is set to the root node
         HybridPageGuard<btree::BTreeNode> rootPageGuard = HybridPageGuard<btree::BTreeNode>(p_guard, p_guard->upper);
         // lock the root of the Btree exclusive, since it is moved to/merged with level below
         ExclusivePageGuard<btree::BTreeNode> rootPageGuardExclusive = ExclusivePageGuard<btree::BTreeNode>(std::move(rootPageGuard));

         // get the btree of level 0, if one exists
         btree::BTreeNode* old = tiers.size() ? (btree::BTreeNode*)((btree::BTreeNode*)tiers[0]->tree.meta_node_bf->page.dt)->upper.bf->page.dt : nullptr;
         // merge the two trees / pull the in-mem-tree down (if only in-mem-tree exists)
         // TODO: check, that newTree can be reached by findParent!
         //<StaticBTree> newTree = make_unique<StaticBTree>();

         unique_ptr<StaticBTree> neu = mergeTrees((btree::BTreeNode*)rootPageGuardExclusive.getBufferFrame()->page.dt, old);
         //cout << "mergeTrees() in insert, lower fence von neu: "<< ((btree::BTreeNode*)neu->tree.meta_node_bf->page.dt)->getLowerFenceKey() << " upper fence: "<< ((btree::BTreeNode*)neu->tree.meta_node_bf->page.dt)->getUpperFenceKey() << endl;


         if (tiers.size()) // some levels are already there
            tiers[0] = move(neu);
         else // only In-Memory exists yet
            tiers.emplace_back(move(neu));

         // nicht mehr nötig, da bereits bei mergeTrees gesetzt worden
         /*
         tiers[0]->tree.type = LSM_TYPE::BTree;
         tiers[0]->tree.level = 0;
         tiers[0]->filter.type = LSM_TYPE::BloomFilter;
         tiers[0]->filter.level = 0;
          */
         ensure(tiers[0]->tree.type == LSM_TYPE::BTree);
         ensure(tiers[0]->filter.type == LSM_TYPE::BloomFilter);
         ensure(tiers[0]->tree.level == 0);
         ensure(tiers[0]->filter.level == 0);


         // generate new empty inMemory BTree
         inMemBTree = make_unique<btree::BTreeLL>();

         // create new metaPage for the inMemory BTree
         auto& newMetaBufferPage = BMC::global_bf->allocatePage();
         Guard guard(newMetaBufferPage.header.latch, GUARD_STATE::EXCLUSIVE);
         newMetaBufferPage.header.keep_in_memory = true;
         newMetaBufferPage.page.dt_id = this->dt_id;
         guard.unlock();

         //create in-memory btree
         inMemBTree->create(this->dt_id, &newMetaBufferPage);
         inMemBTree->type = LSM_TYPE::InMemoryBTree;
         inMemBTree->level = 0;

         // if necessary merge further levels
         mergeAll();
      }
      return result;
}

// searches for an value
OP_RESULT LSM::lookup(u8* key, u16 keyLength, function<void(const u8*, u16)> payload_callback)
{
   //cout << endl << "lookup key = " << key << endl;

   if (inMemBTree->lookup(key, keyLength, payload_callback) == OP_RESULT::OK)
      return OP_RESULT::OK;

   // optional: parallel lookup with queue
   for (unsigned i = 0; i < tiers.size(); i++) {
      if (tiers[i]->filter.lookup(key, keyLength) && tiers[i]->tree.lookup(key, keyLength,payload_callback) == OP_RESULT::OK)
         return OP_RESULT::OK;
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

struct ParentSwipHandler LSM::findParent(void* lsm_object, BufferFrame& to_find)
{
   // check on which level the bufferFrame to find is (page.dt is type btreeNode or BloomFilterPage
   // TODO: btreeNode needs to be of DataStructureIdentifier or we have to find the corresponding BTreeLL!
   DataStructureIdentifier* dsl = static_cast<DataStructureIdentifier*>(reinterpret_cast<DataStructureIdentifier*>(to_find.page.dt));
   u64 toFindLevel = dsl->level;
   LSM_TYPE toFindType = dsl->type;
   LSM* tree = static_cast<LSM*>(reinterpret_cast<LSM*>(lsm_object));

   // check, if merge between two level is ongoing
   if (tree->inMerge) {
      if (toFindType != LSM_TYPE::InMemoryBTree && toFindLevel == tree->levelInMerge) {
         // page to_find can be in old btree at toFindLevel or can be the new created btree
         // TODO check old btree and btree in built
      }
      // else page to_find not in one of the merged trees (can be in the old InMemTree, ...)
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
