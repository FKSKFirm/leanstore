#include "lsm.hpp"
#include <cstdint>
#include <iostream>

using namespace std;
using namespace leanstore::storage::btree;

struct KeyEntry {
   unsigned keyOffset;
   unsigned len;
};

// Compare two string
static int cmpKeys(uint8_t* a, uint8_t* b, unsigned aLength, unsigned bLength) {
   int c = memcmp(a, b, min(aLength, bLength));
   if (c)
      return c;
   return (aLength - bLength);
}

struct BTreeIterator {
   // vector of BTreeNode and position of the current entry in this node (slotID)
   vector<pair<BTreeNode*, int>> stack;

   BTreeIterator(BTreeNode* node) {
      // If no further node or no entries in node, return
      if (!node || (node->is_leaf && node->count==0))
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
   BTreeNode& operator*() const { return *stack.back().first; }
   BTreeNode* operator->() const { return &(operator*()); }

   // redefinition of operator ++ behaves like adding the next BTree nodes to the stack
   BTreeIterator& operator++() {
      while (!stack.empty()) {
         // get the last BTreeNode in the stack
         BTreeNode* node = stack.back().first;
         // get the latest position in this BTreeNode
         int& pos = stack.back().second;
         if (node->is_leaf) {
            // leaf node
            if (pos+1 < node->count) {
               // next entry in leaf
               pos++;
               return *this;
            }
            // remove leaf node when all node entries of him are processed
            stack.pop_back();
         } else {
            // inner node
            if (pos+1 < node->count) {
               // down
               pos++;
               // add child at pos to the stack
               stack.push_back({node->getChild(pos), -1});
            } else if (pos+1 == node->count) {
               // down (upper)
               pos++;
               // add last child with the highest values to the stack
               stack.push_back({node->upper, -1});
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

unsigned spaceNeeded(unsigned prefix, unsigned totalKeySize, unsigned count) {
   // spaceNeeded = BTreeNodeHeader + slots * count + totalKeyAndPayloadSize + lowerFenxe + upperFence
   return sizeof(BTreeNodeHeader) + (sizeof(BTreeNode::Slot) * count) + totalKeySize - (count * prefix) + prefix;
}

// returns the number of same letters
unsigned commonPrefix(uint8_t* a, unsigned lenA, uint8_t* b, unsigned lenB) {
   unsigned limit = min(lenA, lenB);
   unsigned i;
   for (i=0; i<limit; i++)
      if (a[i]!=b[i])
         break;
   return i;
}

void buildNode(vector<KeyEntry>& entries, vector<uint8_t>& keyStorage, BTreeLL& btree) {
   BTreeNode* node = BTreeNode::makeLeaf();
   node->prefix_length = commonPrefix(entries.back().keyOffset+keyStorage.data(), entries.back().len, entries.front().keyOffset+keyStorage.data(), entries.front().len);
   for (unsigned i=0; i<entries.size(); i++)
      node->storeKeyValue(i, entries[i].keyOffset+keyStorage.data(), entries[i].len, nullptr, 0);
   node->count = entries.size();
   node->insertFence(node->lower_fence, entries.back().keyOffset+keyStorage.data(), node->prefix_length); // XXX: could put prefix before last key and set upperfence
   node->makeHint();
   //keyValueDataStore.insert(entries.back().keyOffset + keyStorage.data(), entries.back().len,
   btree.insertLeafSorted(entries.back().keyOffset+keyStorage.data(), entries.back().len, node);
}

// adds the prefix and the key to keyStorage and returns the length of prefix and key
unsigned bufferKey(BTreeIterator& a, vector<uint8_t>& keyStorage) {
   // insert the Prefix at the end of keyStorage
   unsigned pl = a->prefix_length;
   keyStorage.insert(keyStorage.end(), a->getLowerFenceKey(), a->getLowerFenceKey() + pl);

   // insert the key in the keyStorage
   unsigned kl = a.getKeyLen();
   keyStorage.insert(keyStorage.end(), a.getKey(), a.getKey() + kl);

   return pl+kl;
}

unique_ptr<StaticBTree> mergeTrees(BTreeNode* aTree, BTreeNode* bTree) {
   auto newTree = make_unique<StaticBTree>();
   newTree->tree.root->isLeaf = false;
   newTree->tree.root->upper = BTreeNode::makeLeaf();
   newTree->tree.pageCount++;
   uint64_t entryCount = 0;

   unsigned totalKeySize = 0;
   // saves the keyOffset and the length of the key in the keyStorage
   vector<KeyEntry> entries;
   // saves the complete keys in one big list
   vector<uint8_t> keyStorage; keyStorage.reserve(btreePageSize *2);

   BTreeIterator a(aTree); BTreeIterator b(bTree);
   vector<uint8_t> keyA, keyB; keyA.reserve(btreePageSize); keyB.reserve(btreePageSize);

   vector<uint64_t> hashes;

   while (true) {
      BTreeIterator* it;

      if (a.done()) {
         if (b.done()) {
            // all entries processed, create last page and exit
            entryCount += entries.size();
            buildNode(entries, keyStorage, newTree->tree);

            //create BloomFilter
            newTree->filter.init(entryCount);
            for (auto h : hashes)
               newTree->filter.insert(h);

            return newTree;
         }
         it = &b;
      } else if (b.done()) {
         it = &a;
      } else { // take smaller element
         keyA.resize(0); bufferKey(a, keyA);
         keyB.resize(0); bufferKey(b, keyB);
         it = cmpKeys(keyA.data(), keyB.data(), keyA.size(), keyB.size())<0 ? &a : &b;
      }
      unsigned offset = keyStorage.end()-keyStorage.begin();
      unsigned len = bufferKey(*it, keyStorage);
      entries.push_back({offset, len});
      uint8_t* key = keyStorage.data()+keyStorage.size()-len;
      hashes.push_back(BloomFilter::hashKey(key, len));

      unsigned prefix = commonPrefix(entries.front().keyOffset+keyStorage.data(), entries.front().len, key, len);
      if (spaceNeeded(prefix, totalKeySize+len, entries.size()) >= btreePageSize) {
         // key does not fit, create new page
         entries.pop_back();
         entryCount += entries.size();
         buildNode(entries, keyStorage, newTree->tree); // could pick shorter separator here
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
   LSM::LSM() : inMemBTree(std::make_unique<BTreeLL>()) {}

   LSM::~LSM() {}

   void LSM::printLevels() {
      uint64_t sum = 0;
      for (auto& t : tiers) {
         sum += t->tree.countPages();
         cout << t->tree.countPages() << " ";
      }
      cout << " sum:" << sum << endl;
   }

   // merges
   void LSM::mergeAll() {
      uint64_t limit = baseLimit*factor;
      for (unsigned i=0; i<tiers.size(); i++) {
         // curr = Btree on level i
         StaticBTree& curr = *tiers[i];
         if (curr.tree.countPages() >= limit) {
            // current observed BTree is too large
            if (i+1 < tiers.size()) {
               // merge with next level
               StaticBTree& next = *tiers[i+1];
               tiers[i+1] = mergeTrees(curr.tree.root, next.tree.root);
            }
            else {
               // new level
               tiers.emplace_back(move(tiers.back()));
            }
            tiers[i] = make_unique<StaticBTree>();
         }
         limit = limit * factor;
      }
   }

   void LSM::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength) {
      inMemBTree->insert(key, keyLength, payload, payloadLength);
      if (inMemBTree->countPages() >= baseLimit) {
         // merge inMemory-BTree with first level BTree of LSM Tree
         BTreeNode* old = tiers.size() ? tiers[0]->tree.root : nullptr;
         unique_ptr<StaticBTree> neu = mergeTrees(inMemBTree->root, old);
         if (tiers.size())
            tiers[0] = move(neu);
         else
            tiers.emplace_back(move(neu));
         // generate new empty inMemory BTree
         inMemBTree = make_unique<BTreeLL>();
         // if necessary merge further levels
         mergeAll();
      }
   }

   // returns true when the key is found in the inMemory BTree (Root of LSM-Tree)
   bool LSM::lookup(uint8_t* key, unsigned keyLength) {
      if (inMemBTree->lookup(key, keyLength))
         return true;

      for (unsigned i=0; i<tiers.size(); i++) {
         if (tiers[i]->filter.lookup(key, keyLength) && tiers[i]->tree.lookup(key, keyLength))
            return true;
      }

      return false;
   }

   // returns true when the key is found in the BloomFilter or in the inMemBTree
   bool LSM::lookupOnlyBloomFilter(uint8_t* key, unsigned keyLength) {
      if (inMemBTree->lookup(key, keyLength))
         return true;

      for (unsigned i=0; i<tiers.size(); i++) {
         if (tiers[i]->filter.lookup(key, keyLength))
            return true;
      }
      return false;
}
