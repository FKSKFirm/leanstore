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
         struct LSMBTreeMergeIteratorNew {

            btree::BTreeGeneric& btree;
            btree::BTreeGeneric& newBtree;
            HybridPageGuard<btree::BTreeNode> leaf;
            s32 positionInNode = -1;
            bool done = false;

            BufferFrame* oldRootsUpper;

            LSMBTreeMergeIteratorNew(btree::BTreeGeneric& oldTree, btree::BTreeGeneric& newTree) : btree(oldTree), newBtree(newTree) {
               oldRootsUpper = ((btree::BTreeNode*)((btree::BTreeNode*)oldTree.meta_node_bf->page.dt)->upper.bfPtr()->page.dt)->upper.bf;
            }

            void moveToFirstLeaf() {
               leaf.unlock();
               HybridPageGuard<btree::BTreeNode> parent_guard(btree.meta_node_bf);
               HybridPageGuard current_node(parent_guard, parent_guard->upper);
               assert(current_node.ptr()->upper.bfPtr() == oldRootsUpper);

               while (!current_node->is_leaf) {
                  // go one level deeper
                  parent_guard = std::move(current_node);
                  Swip<btree::BTreeNode>& childToFollow = parent_guard->upper;
                  if (parent_guard->count > 0) {
                     childToFollow = parent_guard->getChild(0);
                  }
                  current_node = HybridPageGuard(parent_guard, childToFollow);
               }
               assert(leaf.bufferFrame != current_node.bufferFrame);
               leaf = std::move(current_node);
               positionInNode = 0;
            }

            bool nextKV() {
               leaf->getKey(positionInNode);
               leaf->getKeyLen(positionInNode);
               leaf->getPayload(positionInNode);
               leaf->getPayloadLength(positionInNode);

               assert(leaf->is_leaf);
               if (positionInNode+1 < leaf->count) {
                  positionInNode++;
                  return true;
               }
               else {
                  positionInNode = 0;
                  return nextLeaf();
               }
            }
int counter = 0;
            bool nextLeaf() {
               // findParent(this node) and lock it exclusive
               //    if its the meta_node: return false
               // remove the slot in the parent node
               // reclaim this node
               // if the parent node has more entries:
               //    get the next leaf (follow in every inner node child0 or upper) / could use moveToFirstLeaf as well
               // if the parent node has no more entries:
               //    call nextLeaf()
counter++;
               auto parent = btree.findParent(btree, *leaf.bufferFrame);
               HybridPageGuard<btree::BTreeNode> parentNodeGuard = parent.getParentReadPageGuard<btree::BTreeNode>();
               parentNodeGuard.toExclusive();

               if (parent.parent_bf == btree.meta_node_bf) {
                  // merge done
                  // lock root of new Tree and delete upper ptr reference to this node
                  // get the highest child in the root of the new Tree and set this as upper ptr, decrement count (check for a count == 1, then the root is not necessary)
                  // new Tree is now ready
                  // reclaim
                  if (newBtree.meta_node_bf != btree.meta_node_bf) {
                     auto parentInNewTree = newBtree.findParent(newBtree, *parent.parent_bf);
                     HybridPageGuard<btree::BTreeNode> parentInNewTreePageGuard = parentInNewTree.getParentReadPageGuard<btree::BTreeNode>();
                     parentInNewTreePageGuard.toExclusive();
                     parentInNewTreePageGuard->upper = parentInNewTreePageGuard->getChild(parentInNewTreePageGuard->count - 1);
                     parentInNewTreePageGuard->removeSlot(parentInNewTreePageGuard->count - 1);
                     parentNodeGuard.reclaim();
                  }
                  done = true;
                  leaf.toExclusive();
                  leaf.reclaim();
                  return false;
               }
               else {
                  leaf.toExclusive();
                  leaf.reclaim();
                  if (parentNodeGuard.ptr()->count == 0) {
                     // the leaf was pointed by the upper ptr, so we can reclaim this parent as well
                     leaf = std::move(parentNodeGuard);
                     return nextLeaf();
                  }
                  else {
                     parentNodeGuard.ptr()->removeSlot(0);
                     parentNodeGuard.unlock();
                     moveToFirstLeaf();
                     return true;
                  }
               }
            }

            bool getDeletionFlag() { return leaf->isDeleted(positionInNode); }
            uint8_t* getKey() { return leaf->getKey(positionInNode); }
            unsigned getKeyLen() { return leaf->slot[positionInNode].key_len; }
            uint8_t* getPayload() { return leaf->getPayload(positionInNode); }
            unsigned getPayloadLen() { return leaf->getPayloadLength(positionInNode); }
         };





         struct LSMBTreeMergeIterator {
            // vector of BTreeNode and position of the current entry in this node (slotID)
            JMUW<vector<HybridPageGuard<btree::BTreeNode>*>> nodeList;
            JMUW<vector<int>> positions;


            LSMBTreeMergeIterator(HybridPageGuard<btree::BTreeNode>& node)
            {
               // If no further node or no entries in node, return
               if ((((btree::BTreeNode*)node.bufferFrame->page.dt)->is_leaf && ((btree::BTreeNode*)node.bufferFrame->page.dt)->count == 0))
                  return;
               // add the node to the stack
               //nodeList->push_back(node);
               positions->push_back(-1);
               // move to the next entry
               ++(*this);
            }

            bool done() { return nodeList->empty(); }
            bool getDeletionFlag() { return ((btree::BTreeNode*)nodeList->back()->bufferFrame->page.dt)->isDeleted(positions->back()); }
            uint8_t* getKey() { return ((btree::BTreeNode*)nodeList->back()->bufferFrame->page.dt)->getKey(positions->back()); }
            unsigned getKeyLen() { return ((btree::BTreeNode*)nodeList->back()->bufferFrame->page.dt)->slot[positions->back()].key_len; }
            uint8_t* getPayload() { return ((btree::BTreeNode*)nodeList->back()->bufferFrame->page.dt)->getPayload(positions->back()); }
            unsigned getPayloadLen() { return ((btree::BTreeNode*)nodeList->back()->bufferFrame->page.dt)->getPayloadLength(positions->back()); }

            // returns the last BTreeNode in the stack
            HybridPageGuard<btree::BTreeNode>& operator*() { return *nodeList.obj.back(); }
            HybridPageGuard<btree::BTreeNode>* operator->() { return &(operator*()); }

            // redefinition of operator ++ behaves like adding the next BTree nodes to the stack
            LSMBTreeMergeIterator& operator++()
            {
               while (!nodeList->empty()) {
                  // get the last BTreeNode in the stack
                  HybridPageGuard<btree::BTreeNode>* node = nodeList->back();
                  node->recheck();
                  // get the latest position in this BTreeNode
                  int& pos = positions->back();
                  if (((btree::BTreeNode*)node->bufferFrame->page.dt)->is_leaf) {
                     // leaf node
                     if (pos + 1 < ((btree::BTreeNode*)node->bufferFrame->page.dt)->count) {
                        // next entry in leaf
                        pos++;
                        return *this;
                     }
                     // remove leaf node when all node entries of him are processed
                     while(true) {
                        jumpmuTry()
                           {
                              node->toExclusive();
                              node->reclaim();
                              nodeList->pop_back();
                              positions->pop_back();
                              jumpmu_break;
                           }
                        jumpmuCatch() {}
                     }
                  } else {
                     // inner node
                     if (pos + 1 < ((btree::BTreeNode*)node->bufferFrame->page.dt)->count) {
                        // down
                        pos++;
                        // add child at pos to the stack
                        Swip<btree::BTreeNode> oneChild = ((btree::BTreeNode*)node->bufferFrame->page.dt)->getChild(pos);
                        HybridPageGuard oneChildPG = HybridPageGuard<btree::BTreeNode>(oneChild.bf);
                        oneChildPG.guard.state = GUARD_STATE::MOVED;
                        nodeList->push_back(&oneChildPG);
                        positions->push_back(-1);
                     } else if (pos + 1 == ((btree::BTreeNode*)node->bufferFrame->page.dt)->count) {
                        // down (upper)
                        pos++;
                        // add last child with the highest values to the stack
                        Swip<btree::BTreeNode> oneChild = ((btree::BTreeNode*)node->bufferFrame->page.dt)->upper;
                        HybridPageGuard oneChildPG = HybridPageGuard<btree::BTreeNode>(oneChild.bf);
                        oneChildPG.guard.state = GUARD_STATE::MOVED;
                        nodeList->push_back(&oneChildPG);
                        positions->push_back(-1);
                     } else {
                        // up
                        while(true) {
                           jumpmuTry()
                           {
                              node->toExclusive();
                              node->reclaim();
                              nodeList->pop_back();
                              positions->pop_back();
                              jumpmu_break;
                           }
                           jumpmuCatch() {}
                        }
                     }
                  }
               }
               return *this;
            }
         };
      }
   }
}
