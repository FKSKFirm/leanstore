#ifndef BTREE_LSMTREE_BTREENODE_HPP
#define BTREE_LSMTREE_BTREENODE_HPP

#include <cstdint>
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"

using namespace leanstore::storage;

// -------------------------------------------------------------------------------------

namespace leanstore
{
namespace storage
{
namespace lsmTree
{

struct BTreeNode;
static const unsigned btreePageSize = 4096;
using SwipType = Swip<BTreeNode>;

struct FenceKeySlot {
   uint16_t offset;
   uint16_t length;
};
struct BTreeNode;

struct BTreeNodeHeader {
   static const unsigned underFullSize = btreePageSize / 4;  // merge nodes below this size

   // upper = highest ("most right") node pointer to child in Inner B+ Tree
   Swip<BTreeNode> upper = nullptr;  // only used in inner nodes

   // smallest value in the node which could occur
   FenceKeySlot lowerFence = {0, 0};  // exclusive

   // highest value in the node which could occur
   FenceKeySlot upperFence = {0, 0};  // inclusive

   // number of entries
   uint16_t count = 0;

   // LeafNode with real payload?
   bool isLeaf;

   uint16_t spaceUsed = 0;

   // address for new payload (growing from the back of the page)
   uint16_t dataOffset = static_cast<uint16_t>(btreePageSize);

   // prefix of the value which share the node
   // (e.g. one for 'e' with values 'ea', 'eg', 'esa')
   // Question: Where is the prefix stored?
   uint16_t prefixLength = 0;

   // number of hints
   static const unsigned hintCount = 16;
   uint32_t hint[hintCount];

   BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
   ~BTreeNodeHeader() {}
};

struct BTreeNode : public BTreeNodeHeader {
   // slot is used for storing data
   struct Slot {
      uint16_t offset;
      uint16_t keyLen;
      uint16_t payloadLen;
      uint32_t head;
   } __attribute__((packed));

   union {
      // slot is the control block for payload data
      Slot slot[(btreePageSize - sizeof(BTreeNodeHeader)) / sizeof(Slot)];  // grows from front
      // heap is the container for payload data
      uint8_t heap[btreePageSize - sizeof(BTreeNodeHeader)];  // grows from back
   };

   // maxKeySize for at least two entries of payload
   static constexpr unsigned maxKeySize = ((btreePageSize - sizeof(BTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

   BTreeNode(bool isLeaf);

   bool isInner();

   // Pointer to the lowest and highest value in the BTreeNode
   uint8_t* getLowerFence();

   // get the common prefix of the key
   uint8_t* getPrefix();  // any key on page is ok

   unsigned freeSpaceAfterCompaction();

   bool requestSpaceFor(unsigned spaceNeeded);

   // generates new leaf node
   static BTreeNode* makeLeaf() { return new BTreeNode(true); }
   // generates new inner node
   static BTreeNode* makeInner() { return new BTreeNode(false); }

   uint8_t* getKey(unsigned slotId);
   uint8_t* getPayload(unsigned slotId);

   // applies only to inner nodes, returns the child BTreeNode
   //BTreeNode* getChild(unsigned slotId);
   inline SwipType& getChild(u16 slotId) { return *reinterpret_cast<SwipType*>(getPayload(slotId)); }

   // How much space would inserting a new key of length "keyLength" require?
   unsigned spaceNeeded(unsigned keyLength, unsigned payloadLength);

   // ===== HINTS BEGIN =====

   void makeHint();

   // ===== HINTS END =====

   // lower bound search, foundOut indicates if there is an exact match, returns slotId
   unsigned lowerBound(uint8_t* key, unsigned keyLength, bool& foundOut);

   // lowerBound wrapper ignoring exact match argument (for convenience)
   unsigned lowerBound(uint8_t* key, unsigned keyLength);

   bool insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   bool remove(uint8_t* key, unsigned keyLength);

   // merge right node into this node
   bool mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right);

   // store key/value pair at slotId
   void storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   void insertFence(FenceKeySlot& fk, uint8_t* key, unsigned keyLength);

   void splitNode(BTreeNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength);

   struct SeparatorInfo {
      unsigned length;   // length of new separator
      unsigned slot;     // slot at which we split
      bool isTruncated;  // if true, we truncate the separator taking length bytes from slot+1
   };

   SeparatorInfo findSeparator();

   void getSep(uint8_t* sepKeyOut, SeparatorInfo info);

   Swip<BTreeNode>& lookupInner(uint8_t* key, unsigned keyLength);

   void destroy();

   uint8_t* getUpperFence();

  private:
   unsigned commonPrefix(unsigned slotA, unsigned slotB);
   void setFences(uint8_t* lowerKey, unsigned lowerLen, uint8_t* upperKey, unsigned upperLen);
   void copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount);
   void copyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot);
   void compactify();
   bool removeSlot(unsigned slotId);
   unsigned freeSpace();
   // Pointer to the begin of the BTreeNode
   uint8_t* ptr();

   void updateHint(unsigned slotId);
   void searchHint(uint32_t keyHead, unsigned& lowerOut, unsigned& upperOut);
};

}
}
}
#endif  // BTREE_LSMTREE_BTREENODE_HPP
