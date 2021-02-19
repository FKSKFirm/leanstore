#include "btree.hpp"
#include <cassert>
#include <iostream>
#include "btreeNode.hpp"

using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace lsmTree
{

BTree::BTree() : root(BTreeNode::makeLeaf()), pageCount(1) {}

BTree::~BTree()
{
   root->destroy();
}

// point lookup
uint8_t* BTree::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
   BTreeNode* node = root;
   while (node->isInner())
      node = node->lookupInner(key, keyLength);
   bool found;
   unsigned pos = node->lowerBound(key, keyLength, found);
   if (!found)
      return nullptr;

   // key found, copy payload
   assert(pos < node->count);
   payloadSizeOut = node->slot[pos].payloadLen;
   /*
    * Test for String/char/uint_8 conversion:
      std::string teststring = "Stringtest";
      char* test = "CharPioneeHalllo";
      uint8_t* uint8Test = (uint8_t*)"Uint8Hallo";
   */
   /*
    * Test for results of the head function
      uint8_t* headTest0 = (uint8_t*)"";
      std::cout << std::hex << head(headTest0, 0) << std::endl;

      uint8_t* headTest1 = (uint8_t*)"A";
      std::cout  << std::hex << head(headTest1, 1) << std::endl;

      uint8_t* headTest2 = (uint8_t*)"EG";
      std::cout << std::hex << loadUnaligned<uint16_t>(headTest2) << std::endl;
      std::cout  << std::hex << head(headTest2, 2) << std::endl;

      uint8_t* headTest3 = (uint8_t*)"ABC";
      std::cout << std::hex << loadUnaligned<uint16_t>(headTest3) << std::endl;
      std::cout << head(headTest3, 3) << std::endl;

      uint8_t* headTest4 = (uint8_t*)"ABCD";
      std::cout << std::hex << loadUnaligned<uint32_t>(headTest4) << std::endl;
      std::cout << head(headTest4, 4) << std::endl;

      uint8_t* headTest5 = (uint8_t*)"ABCDE";
      std::cout << std::hex << loadUnaligned<uint32_t>(headTest5) << std::endl;
      std::cout << head(headTest5, 5) << std::endl;
   */
   /* Remove comment to see what is inserted
      std::string keyPrefixString;
      keyPrefixString.assign(node->getPrefix(), node->getPrefix() + node->prefixLength);
      std::string keyString;
      keyString.assign(node->getKey(pos), node->getKey(pos) + keyLength - node->prefixLength);
      std::string payloadString;
      payloadString.assign(node->getPayload(pos), node->getPayload(pos) + payloadSizeOut);
      std::uint64_t payloadInt = *reinterpret_cast<uint64_t*>(node->getPayload(pos));

      // For string-payload:
      // std::cout << "KEY: \t" << keyPrefixString << " | " << keyString << "\n Payload: \t" << payloadString << " " << std::endl;
      // For integer-payload:
      std::cout << "KEY: \t" << keyPrefixString << " | " << keyString << "\n Payload: \t" << payloadInt << " " << std::endl;
   */
   return node->getPayload(pos);
}

bool BTree::lookup(uint8_t* key, unsigned keyLength)
{
   unsigned x;
   return lookup(key, keyLength, x) != nullptr;
}

void BTree::splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength, unsigned payloadLength)
{
   // create new root if necessary
   if (!parent) {
      // root is an inner node
      parent = BTreeNode::makeInner();
      pageCount++;
      // link the upper pointer to the location of the old node, which will be replaced through the new splitted right node
      parent->upper = node;
      // set the new root node of the BTree
      root = parent;
   }

   // split
   BTreeNode::SeparatorInfo sepInfo = node->findSeparator();
   // space needed in parent node is the size of the seperator (maybe part of the key) and the payload, which is the pointer to the child node
   unsigned spaceNeededParent = parent->spaceNeeded(sepInfo.length, payloadLength);
   if (parent->requestSpaceFor(spaceNeededParent)) {  // is there enough space in the parent for the separator?
      uint8_t sepKey[sepInfo.length];
      node->getSep(sepKey, sepInfo);
      pageCount++;
      node->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
   } else {
      // must split parent first to make space for separator, restart from root to do this
      ensureSpace(parent, key, keyLength, payloadLength);
   }
}

// make place for new insert, split nodes
void BTree::ensureSpace(BTreeNode* toSplit, uint8_t* key, unsigned keyLength, unsigned payloadLength)
{
   BTreeNode* node = root;
   BTreeNode* parent = nullptr;
   while (node->isInner() && (node != toSplit)) {
      parent = node;
      node = node->lookupInner(key, keyLength);
   }
   splitNode(toSplit, parent, key, keyLength, payloadLength);
}

void BTree::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
   assert(keyLength <= BTreeNode::maxKeySize);
   BTreeNode* node = root;
   BTreeNode* parent = nullptr;
   while (node->isInner()) {
      parent = node;
      node = node->lookupInner(key, keyLength);
   }
   if (node->insert(key, keyLength, payload, payloadLength))
      return;

   // node is full: split and restart
   splitNode(node, parent, key, keyLength, payloadLength);
   insert(key, keyLength, payload, payloadLength);
}

void BTree::insertLeafSorted(uint8_t* key, unsigned keyLength, BTreeNode* leaf)
{
   BTreeNode* node = root;
   BTreeNode* parent = nullptr;
   BTreeNode* parentParent = nullptr;
   while (node->isInner()) {
      parentParent = parent;
      parent = node;
      node = node->lookupInner(key, keyLength);
   }
   if (parent->insert(key, keyLength, reinterpret_cast<uint8_t*>(&leaf), sizeof(BTreeNode*))) {
      pageCount++;
      return;
   }
   // no more space, need to split
   splitNode(parent, parentParent, key, keyLength, sizeof(BTreeNode*));
   insertLeafSorted(key, keyLength, leaf);
}

bool BTree::remove(uint8_t* key, unsigned keyLength)
{
   BTreeNode* node = root;
   BTreeNode* parent = nullptr;
   unsigned pos = 0;
   while (node->isInner()) {
      parent = node;
      pos = node->lowerBound(key, keyLength);
      node = (pos == node->count) ? node->upper : node->getChild(pos);
   }
   if (!node->remove(key, keyLength))
      return false;  // key not found

   // merge if underfull
   if (node->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
      // find neighbor and merge
      if (parent && (parent->count >= 2) && ((pos + 1) < parent->count)) {
         BTreeNode* right = parent->getChild(pos + 1);
         if (right->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
            pageCount -= node->mergeNodes(pos, parent, right);
            return true;
         }
      }
   }
   return true;
}
// TODO for LeanStore:
/*
struct ParentSwipHandler BTree::findParent(void* btree_object, BufferFrame& to_find)
{
   // Pre: bufferFrame is write locked TODO: but trySplit does not ex lock !
   auto& c_node = *reinterpret_cast<BTreeNode*>(to_find.page.dt);
   auto& btree = *reinterpret_cast<BTree*>(btree_object);
   // -------------------------------------------------------------------------------------
   HybridPageGuard<BTreeNode> p_guard(btree.meta_node_bf);
   uint16_t level = 0;
   // -------------------------------------------------------------------------------------
   Swip<BTreeNode>* c_swip = &p_guard->upper;
   if (btree.dt_id != to_find.page.dt_id || (!p_guard->upper.isHOT())) {
      jumpmu::jump();
   }
   // -------------------------------------------------------------------------------------
   const bool infinity = c_node.upper_fence.offset == 0;
   const uint16_t key_length = c_node.upper_fence.length;
   uint8_t * key = c_node.getUpperFenceKey();
   // -------------------------------------------------------------------------------------
   // check if bufferFrame is the root node
   if (c_swip->bfPtrAsHot() == &to_find) {
      p_guard.recheck();
      return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(p_guard.guard), .parent_bf = btree.meta_node_bf};
   }
   // -------------------------------------------------------------------------------------
   HybridPageGuard c_guard(p_guard, p_guard->upper);  // the parent of the bufferFrame we are looking for (to_find)
   int16_t pos = -1;
   auto search_condition = [&]() {
     if (infinity) {
        c_swip = &(c_guard->upper);
        pos = c_guard->count;
     } else {
        pos = c_guard->lowerBound<false>(key, key_length);
        if (pos == c_guard->count) {
           c_swip = &(c_guard->upper);
        } else {
           c_swip = &(c_guard->getChild(pos));
        }
     }
     return (c_swip->bfPtrAsHot() != &to_find);
   };
   while (!c_guard->is_leaf && search_condition()) {
      p_guard = std::move(c_guard);
      if (c_swip->isEVICTED()) {
         jumpmu::jump();
      }
      c_guard = HybridPageGuard(p_guard, c_swip->cast<BTreeNode>());
      level++;
   }
   p_guard.kill();
   const bool found = c_swip->bfPtrAsHot() == &to_find;
   c_guard.recheck();
   if (!found) {
      jumpmu::jump();
   }
   return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(c_guard.guard), .parent_bf = c_guard.getBufferFrame, .pos = pos};
}
 */


// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTree::getMeta()
{
   DTRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
       .find_parent = findParent,
       .check_space_utilization = checkSpaceUtilization,
       .checkpoint = checkpoint,
       .undo = undo,
       .todo = todo};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
// Called by buffer manager before eviction
// Returns true if the buffer manager has to restart and pick another buffer frame for eviction
// Attention: the guards here down the stack are not synchronized with the ones in the buffer frame manager stack frame
bool BTree::checkSpaceUtilization(void* btree_object, BufferFrame& bf, OptimisticGuard& o_guard, ParentSwipHandler& parent_handler)
{
   //TODO: If Xmerge is active
   return false;
}
// -------------------------------------------------------------------------------------
void BTree::checkpoint(void*, BufferFrame& bf, u8* dest)
{
   std::memcpy(dest, bf.page.dt, EFFECTIVE_PAGE_SIZE);
   auto node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
   auto dest_node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
   if (!node.isLeaf) {
      for (u64 t_i = 0; t_i < dest_node.count; t_i++) {
         if (!dest_node.getChild(t_i).isEVICTED()) {
            auto& bf = dest_node.getChild(t_i).bfRefAsHot();
            dest_node.getChild(t_i).evict(bf.header.pid);
         }
      }
      if (!dest_node.upper.isEVICTED()) {
         auto& bf = dest_node.upper.bfRefAsHot();
         dest_node.upper.evict(bf.header.pid);
      }
   }
}
// -------------------------------------------------------------------------------------
// Jump if any page on the path is already evicted or of the getBufferFrame could not be found
// to_find is not latched
struct ParentSwipHandler BTree::findParent(void* btree_object, BufferFrame& to_find)
{
   BTree& btree = *static_cast<BTree*>(reinterpret_cast<BTree*>(btree_object));
   auto& c_node = *reinterpret_cast<BTreeNode*>(to_find.page.dt);
   // -------------------------------------------------------------------------------------
   HybridPageGuard<BTreeNode> p_guard(btree.meta_node_bf);
   u16 level = 0;
   // -------------------------------------------------------------------------------------
   Swip<BTreeNode>* c_swip = &p_guard->upper;
   if (btree.dt_id != to_find.page.dt_id || (!p_guard->upper.isHOT())) {
      jumpmu::jump();
   }
   // -------------------------------------------------------------------------------------
   const bool infinity = c_node.upperFence.offset == 0;
   const u16 key_length = c_node.upperFence.length;
   u8* key = c_node.getUpperFence();
   // -------------------------------------------------------------------------------------
   // check if getBufferFrame is the root node
   if (c_swip->bfPtrAsHot() == &to_find) {
      p_guard.recheck();
      return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(p_guard.guard), .parent_bf = btree.meta_node_bf};
   }
   // -------------------------------------------------------------------------------------
   HybridPageGuard c_guard(p_guard, p_guard->upper);  // the parent of the getBufferFrame we are looking for (to_find)
   s16 pos = -1;
   auto search_condition = [&]() {
     if (infinity) {
        c_swip = &(c_guard->upper);
        pos = c_guard->count;
     } else {
        pos = c_guard->lowerBound<false>(key, key_length);
        if (pos == c_guard->count) {
           c_swip = &(c_guard->upper);
        } else {
           c_swip = &(c_guard->getChild(pos));
        }
     }
     return (c_swip->bfPtrAsHot() != &to_find);
   };
   while (!c_guard->isLeaf && search_condition()) {
      p_guard = std::move(c_guard);
      if (c_swip->isEVICTED()) {
         jumpmu::jump();
      }
      c_guard = HybridPageGuard(p_guard, c_swip->cast<BTreeNode>());
      level++;
   }
   p_guard.unlock();
   const bool found = c_swip->bfPtrAsHot() == &to_find;
   c_guard.recheck();
   if (!found) {
      jumpmu::jump();
   }
   return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(c_guard.guard), .parent_bf = c_guard.bufferFrame, .pos = pos};
}
// -------------------------------------------------------------------------------------
void BTree::iterateChildrenSwips(void*, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   // Pre: getBufferFrame is read locked
   auto& c_node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
   if (c_node.isLeaf) {
      return;
   }
   for (u16 i = 0; i < c_node.count; i++) {
      if (!callback(c_node.getChild(i).cast<BufferFrame>())) {
         return;
      }
   }
   callback(c_node.upper.cast<BufferFrame>());
}
// -------------------------------------------------------------------------------------
void BTree::undo(void*, const u8*, const u64) {}
void BTree::todo(void*, const u8*, const u64) {}
// -------------------------------------------------------------------------------------

}
}
}
