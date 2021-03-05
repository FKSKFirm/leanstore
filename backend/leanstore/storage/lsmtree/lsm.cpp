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
   vector<pair<btree::BTreeNode*, int>> stack;

   BTreeIterator(btree::BTreeNode* node)
   {
      // If no further node or no entries in node, return
      if (!node || (node->is_leaf && node->count == 0))
         return;
      // add the node to the stack
      stack.push_back({node, -1});
      // move to the next entry
      ++(*this);
   }

   bool done() { return stack.empty(); }
   uint8_t* getKey() { return stack.back().first->getKey(stack.back().second); }
   unsigned getKeyLen() { return stack.back().first->slot[stack.back().second].key_len; }

   // returns the last BTreeNode in the stack
   btree::BTreeNode& operator*() const { return *stack.back().first; }
   btree::BTreeNode* operator->() const { return &(operator*()); }

   // redefinition of operator ++ behaves like adding the next BTree nodes to the stack
   BTreeIterator& operator++()
   {
      while (!stack.empty()) {
         // get the last BTreeNode in the stack
         btree::BTreeNode* node = stack.back().first;
         // get the latest position in this BTreeNode
         int& pos = stack.back().second;
         if (node->is_leaf) {
            // leaf node
            if (pos + 1 < node->count) {
               // next entry in leaf
               pos++;
               return *this;
            }
            // remove leaf node when all node entries of him are processed
            stack.pop_back();
         } else {
            // inner node
            if (pos + 1 < node->count) {
               // down
               pos++;
               // add child at pos to the stack
               stack.push_back({(btree::BTreeNode*)node->getChild(pos).bf->page.dt, -1});
            } else if (pos + 1 == node->count) {
               // down (upper)
               pos++;
               // add last child with the highest values to the stack
               stack.push_back({(btree::BTreeNode*)node->upper.bf->page.dt, -1});
            } else {
               // up
               // TODO: all children are processed and in the stack, remove (inner parent) node?????
               stack.pop_back();
            }
         }
      }
      return *this;
   }
};

unsigned spaceNeeded(unsigned prefix, unsigned totalKeySize, unsigned count)
{
   // spaceNeeded = BTreeNodeHeader + slots * count + totalKeyAndPayloadSize + lowerFenxe + upperFence
   return sizeof(btree::BTreeNodeHeader) + (sizeof(btree::BTreeNode::Slot) * count) + totalKeySize - (count * prefix) + prefix;
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

void buildNode(vector<KeyEntry>& entries, vector<uint8_t>& keyStorage, btree::BTreeLL& btree)
{
   btree::BTreeNode node = btree::BTreeNode(true);
   node.prefix_length = commonPrefix(entries.back().keyOffset + keyStorage.data(), entries.back().len, entries.front().keyOffset + keyStorage.data(),
                                      entries.front().len);
   for (unsigned i = 0; i < entries.size(); i++)
      node.storeKeyValue(i, entries[i].keyOffset + keyStorage.data(), entries[i].len, nullptr, 0);
   node.count = entries.size();
   node.insertFence(node.lower_fence, entries.back().keyOffset + keyStorage.data(),
                     node.prefix_length);  // XXX: could put prefix before last key and set upperfence
   node.makeHint();
   // keyValueDataStore.insert(entries.back().keyOffset + keyStorage.data(), entries.back().len,
   //btree.insertLeafSorted(entries.back().keyOffset + keyStorage.data(), entries.back().len, node);
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

unique_ptr<StaticBTree> mergeTrees(btree::BTreeNode* aTree, btree::BTreeNode* bTree)
{
   auto newTree = make_unique<StaticBTree>();
   ((btree::BTreeNode*)newTree->tree.meta_node_bf->page.dt)->is_leaf = false;
   //((BTreeNode*)newTree->tree.meta_node_bf->page.dt)->upper = BTreeNode::makeLeaf();
   //newTree->tree.pageCount++;
   uint64_t entryCount = 0;

   unsigned totalKeySize = 0;
   // saves the keyOffset and the length of the key in the keyStorage
   vector<KeyEntry> entries;
   // saves the complete keys in one big list
   vector<uint8_t> keyStorage;
   keyStorage.reserve(btree::btreePageSize * 2);

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
            entryCount += entries.size();
            buildNode(entries, keyStorage, newTree->tree);

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
      unsigned offset = keyStorage.end() - keyStorage.begin();
      unsigned len = bufferKey(*it, keyStorage);
      entries.push_back({offset, len});
      uint8_t* key = keyStorage.data() + keyStorage.size() - len;
      hashes.push_back(BloomFilter::hashKey(key, len));

      unsigned prefix = commonPrefix(entries.front().keyOffset + keyStorage.data(), entries.front().len, key, len);
      if (spaceNeeded(prefix, totalKeySize + len, entries.size()) >= btree::btreePageSize) {
         // key does not fit, create new page
         entries.pop_back();
         entryCount += entries.size();
         buildNode(entries, keyStorage, newTree->tree);  // could pick shorter separator here
         entries.clear();
         keyStorage.clear();
         totalKeySize = 0;
      } else {
         // key fits, buffer it
         // go to next entry in the tree
         ++(*it);
         totalKeySize += len;
      }
   }
}
LSM::LSM() : inMemBTree(std::make_unique<btree::BTreeLL>()) {}

LSM::~LSM() {}

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
            tiers[i + 1] = mergeTrees((btree::BTreeNode*)curr.tree.meta_node_bf->page.dt, (btree::BTreeNode*)next.tree.meta_node_bf->page.dt);
         } else {
            // new level
            tiers.emplace_back(move(tiers.back()));
         }
         tiers[i] = make_unique<StaticBTree>();
      }
      limit = limit * factor;
   }
}

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
      // if necessary merge further levels
      mergeAll();
   }
}
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
   // is NULL safe for Deletion marker?
}

OP_RESULT LSM::scanAsc(u8* start_key,
                          u16 key_length,
                          function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
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

OP_RESULT LSM::scanDesc(u8* start_key,
                           u16 key_length,
                           function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
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
   inMemBTree->insert(key, keyLength, payload, payloadLength);
   if (inMemBTree->countPages() >= baseLimit) {
      // merge inMemory-BTree with first level BTree of LSM Tree

      HybridPageGuard<btree::BTreeNode> p_guard(inMemBTree->meta_node_bf);
      // p_guard->upper is root, target_guard set to the root node
      HybridPageGuard<btree::BTreeNode> root = HybridPageGuard<btree::BTreeNode>(p_guard, p_guard->upper);

      btree::BTreeNode* old = tiers.size() ? (btree::BTreeNode*)root.bufferFrame->page.dt : nullptr;
      unique_ptr<StaticBTree> neu = mergeTrees(root.ptr(), old);
      if (tiers.size())
         tiers[0] = move(neu);
      else
         tiers.emplace_back(move(neu));
      // generate new empty inMemory BTree
      inMemBTree = make_unique<btree::BTreeLL>();
      // if necessary merge further levels
      mergeAll();
   }
}

// searches for an value
OP_RESULT LSM::lookup(u8* key, u16 keyLength, function<void(const u8*, u16)> payload_callback)
{
   cout << endl << "lookup key = " << key << endl;

   if (inMemBTree->lookup(key, keyLength, payload_callback) == OP_RESULT::OK)
      return OP_RESULT::OK;

   for (unsigned i = 0; i < tiers.size(); i++) {
      if (tiers[i]->filter.lookup(key, keyLength) && tiers[i]->tree.lookup(key, keyLength,payload_callback) == OP_RESULT::OK)
         return OP_RESULT::OK;
   }

   return OP_RESULT::NOT_FOUND;
}

struct DataTypeRegistry::DTMeta LSM::getMeta()
{
   DataTypeRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
       .find_parent = findParent,
       .check_space_utilization = checkSpaceUtilization};
   return btree_meta;
}

bool LSM::checkSpaceUtilization(void* btree_object, BufferFrame& bf, OptimisticGuard& o_guard, ParentSwipHandler& parent_handler)
{
   return false;
}

void LSM::iterateChildrenSwips(void*, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   // Pre: getBufferFrame is read locked
   auto& c_node = *reinterpret_cast<btree::BTreeNode*>(bf.page.dt);
   if (c_node.is_leaf) {
      return;
   }
   for (u16 i = 0; i < c_node.count; i++) {
      if (!callback(c_node.getChild(i).cast<BufferFrame>())) {
         return;
      }
   }
   callback(c_node.upper.cast<BufferFrame>());
}

struct ParentSwipHandler LSM::findParent(void* btree_object, BufferFrame& to_find)
{
   return btree::BTreeGeneric::findParent(*static_cast<btree::BTreeGeneric*>(reinterpret_cast<btree::BTreeLL*>(btree_object)), to_find);
}

}
}
}
