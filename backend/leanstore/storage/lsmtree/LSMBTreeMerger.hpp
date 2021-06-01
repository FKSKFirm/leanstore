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

            LSMBTreeMerger(btree::BTreeGeneric& oldTree) : btree(oldTree) { }
            ~LSMBTreeMerger() { leaf.unlock(); }

            void moveToFirstLeaf() {
               while (true) {
                  jumpmuTry()
                  {
                     leaf.unlock();
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
                        parent_guard = std::move(current_node);
                        Swip<btree::BTreeNode>* childToFollow;
                        if (parent_guard->count > 0) {
                           childToFollow = &parent_guard->getChild(0);
                        } else {
                           childToFollow = &parent_guard->upper;
                        }
                        current_node = HybridPageGuard(parent_guard, *childToFollow);
                     }
                     if (current_node->count == 0) {
                        done = true;
                        jumpmu_break;
                     }
                     assert(current_node->count > 0);
                     parent_guard.unlock();
                     leaf = std::move(current_node);
                     leaf.toExclusive();
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

            bool nextLeaf() {
               // findParent(this node) and lock it exclusive
               //    if its the meta_node: return false
               // remove the slot in the parent node
               // reclaim this node
               // if the parent node has more entries:
               //    get the next leaf (follow in every inner node child0 or upper) / could use moveToFirstLeaf as well
               // if the parent node has no more entries:
               //    call nextLeaf()
               while (true) {
                  jumpmuTry()
                  {
                     /*const u16 key_length = leaf->upper_fence.length;
                     u8 key[key_length];
                     std::memcpy(key, leaf->getUpperFenceKey(), leaf->upper_fence.length);

                     leaf.unlock();

                     btree.findLeafAndLatchParent<LATCH_FALLBACK_MODE::EXCLUSIVE>(leaf, key, key_length);
                     positionInNode = 0;
*/
                     auto parent = btree.findParent(btree, *leaf.bufferFrame);
                     HybridPageGuard<btree::BTreeNode> parentNodeGuard = parent.getParentReadPageGuard<btree::BTreeNode>();
                     parentNodeGuard.toExclusive();

                     if (parentNodeGuard.bufferFrame == btree.meta_node_bf) {
                        // merge done
                        assert(leaf.bufferFrame != btree.meta_node_bf);
                        leaf.unlock();
                        done = true;
                        jumpmu_return false;
                     }
                     else {
                        leaf.toExclusive();
                        assert(leaf.bufferFrame != btree.meta_node_bf);
                        leaf.reclaim();
                        if (parentNodeGuard.ptr()->count == 0) {
                           // the leaf was pointed by the upper ptr, so we can reclaim this parent as well
                           leaf = std::move(parentNodeGuard);
                           leaf.toExclusive();
                           bool ret = nextLeaf();
                           jumpmu_return ret;
                        }
                        else {
                           parentNodeGuard.ptr()->removeSlot(0);
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
