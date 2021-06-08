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
         struct LSMBTreeMerger {

            btree::BTreeGeneric& btree;
            HybridPageGuard<btree::BTreeNode> leaf;
            s32 positionInNode = -1;
            bool done = false;
            s32 currentLevel = 0;

            LSMBTreeMerger(btree::BTreeGeneric& oldTree) : btree(oldTree) { }
            ~LSMBTreeMerger() { leaf.unlock(); }

            void moveToFirstLeaf() {
               while (true) {
                  jumpmuTry()
                  {
                     leaf.unlock();
                     currentLevel = 0;// 0=root
                     HybridPageGuard<btree::BTreeNode> parent_guard(btree.meta_node_bf);
                     HybridPageGuard current_node(parent_guard, parent_guard->upper);

                     if (current_node->is_leaf && current_node->count == 0) {
                        // empty btree (only root as leaf exists and this node is empty)
                        parent_guard.unlock();
                        done = true;
                        jumpmu_break;
                     }

                     assert(!current_node.ptr()->is_leaf || current_node->count > 0);

                     while (!current_node->is_leaf) {
                        // go one level deeper
                        Swip<btree::BTreeNode>* childToFollow;
                        if (current_node->count > 0) {
                           childToFollow = &current_node->getChild(0);
                        } else {
                           childToFollow = &current_node->upper;
                        }
                        parent_guard = std::move(current_node);
                        current_node = HybridPageGuard(parent_guard, *childToFollow);
                        current_node.bf->header.keep_in_memory = true;
                        currentLevel++;
                     }
                     if (current_node->count == 0) {
                        assert(false);
                        done = true;
                        jumpmu_break;
                     }
                     assert(current_node->count > 0);
                     parent_guard.unlock();
                     leaf = std::move(current_node);
                     leaf.toExclusive();
                     cout << "move21Leaf: " << leaf.bf << " id: " << leaf.ptr()->id << endl;
                     positionInNode = 0;
                     jumpmu_break;
                  }
                  jumpmuCatch() { }
               }
            }

            bool nextKV() {
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

            void findParentAndLatch(HybridPageGuard<btree::BTreeNode>& target_guard)
            {
               while (true) {
                  jumpmuTry()
                     {
                        target_guard.unlock();
                        // get the meta node
                        HybridPageGuard<btree::BTreeNode> p_guard(btree.meta_node_bf);
                        // p_guard->upper is root, target_guard set to the root node
                        target_guard = HybridPageGuard<btree::BTreeNode>(p_guard, p_guard->upper);
                        // -------------------------------------------------------------------------------------
                        u16 volatile level = 0;
                        // -------------------------------------------------------------------------------------
                        // search for the leaf node
                        while (currentLevel > level+1) {
                           // search in current node (target_guard) to correct link to child
                           Swip<btree::BTreeNode>* c_swip;
                           if (target_guard->count > 0) {
                              c_swip = &target_guard->getChild(0);
                           } else {
                              c_swip = &target_guard->upper;
                           }
                           p_guard = std::move(target_guard);
                           target_guard = HybridPageGuard(p_guard, *c_swip);
                           level++;
                        }
                        // -------------------------------------------------------------------------------------
                        p_guard.unlock();
                        target_guard.toExclusive();
                        jumpmu_return;
                     }
                  jumpmuCatch() { }
               }
            }

            bool nextLeaf() {
               // findParent(this node) and lock it exclusive
               //    if its the meta_node: return false
               // remove the slot in the parent node
               // reclaim this node
               // if the parent node has more entries:
               //    get the next leaf (follow in every inner node child0 or upper) / could use moveToFirstLeaf as well
               // if the parent node has no more entries:
               //    call nextLeaf()

               if (currentLevel == 0) {
                  leaf.unlock();
                  done = true;
                  return false;
               }

               while (true) {
                  jumpmuTry()
                  {
                     HybridPageGuard<btree::BTreeNode> parentNodeGuard;
                     findParentAndLatch(parentNodeGuard);

                     if (parentNodeGuard.bf == btree.meta_node_bf) {
                        // merge done
                        assert(leaf.bf != btree.meta_node_bf);
                        leaf.unlock();
                        done = true;
                        jumpmu_return false;
                     }
                     else {
                        leaf.toExclusive();
                        leaf.incrementGSN();
                        assert(leaf.bf != btree.meta_node_bf);
                        cout << "nextLeaf reclaim: " << leaf.bf << " id: " << leaf.ptr()->id << endl;
                        leaf.reclaim();
                        parentNodeGuard.incrementGSN();
                        if (parentNodeGuard->count == 0) {
                           currentLevel--;
                           // the leaf was pointed by the upper ptr, so we can reclaim this parent as well
                           leaf = std::move(parentNodeGuard);
                           leaf.toExclusive();
                           bool ret = nextLeaf();
                           jumpmu_return ret;
                        }
                        else {
                           parentNodeGuard->removeSlot(0);
                           parentNodeGuard.unlock();
                           moveToFirstLeaf();
                           jumpmu_return true;
                        }
                     }
                  }
                  jumpmuCatch() { }
               }
            }

            bool getDeletionFlag() { return leaf->isDeleted(positionInNode); }
            uint8_t* getKey() { return leaf->getKey(positionInNode); }
            unsigned getKeyLen() { return leaf->slot[positionInNode].key_len; }
            uint8_t* getPayload() { return leaf->getPayload(positionInNode); }
            unsigned getPayloadLen() { return leaf->getPayloadLength(positionInNode); }
         };
      }
   }
}
