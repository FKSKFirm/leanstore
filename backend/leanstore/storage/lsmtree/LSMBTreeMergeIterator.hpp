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

            int keyBefore = -1;

            LSMBTreeMergeIteratorNew(btree::BTreeGeneric& oldTree, btree::BTreeGeneric& newTree) : btree(oldTree), newBtree(newTree) { }
            ~LSMBTreeMergeIteratorNew() { leaf.unlock(); }

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
                        //leaf = std::move(current_node);
                        //cout << "Empty root: " << current_node.bufferFrame << " meta node: " << parent_guard.bufferFrame << endl;
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
                     //assert(leaf.bufferFrame != current_node.bufferFrame);
                     parent_guard.unlock();
                     leaf = std::move(current_node);
                     leaf.toExclusive();
                     positionInNode = 0;
                     assert(keyBefore+1 == __builtin_bswap32(*reinterpret_cast<const uint32_t*>(leaf->getKey(positionInNode))) ^ (1ul << 31));
                     jumpmu_break;
                  }
                  jumpmuCatch() { }
               }
            }

            bool nextKV() {
               keyBefore = __builtin_bswap32(*reinterpret_cast<const uint32_t*>(leaf->getKey(positionInNode))) ^ (1ul << 31);
               leaf->getKeyLen(positionInNode);
               leaf->getPayload(positionInNode);
               leaf->getPayloadLength(positionInNode);

               assert(leaf->is_leaf);
               if (positionInNode+1 < leaf->count) {
                  positionInNode++;
                  assert(keyBefore+1 == __builtin_bswap32(*reinterpret_cast<const uint32_t*>(leaf->getKey(positionInNode))) ^ (1ul << 31));
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
                     auto parent = btree.findParent(btree, *leaf.bufferFrame);
                     HybridPageGuard<btree::BTreeNode> parentNodeGuard = parent.getParentReadPageGuard<btree::BTreeNode>();
                     parentNodeGuard.toExclusive();

                     if (parentNodeGuard.bufferFrame == btree.meta_node_bf) {
                        // merge done
                        assert(leaf.bufferFrame != btree.meta_node_bf);
                        leaf.unlock();
                        //leaf.reclaim();
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
