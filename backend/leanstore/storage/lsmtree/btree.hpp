#ifndef BTREE_LSMTREE_BTREE_HPP
#define BTREE_LSMTREE_BTREE_HPP

#include <leanstore/LeanStore.hpp>
#include <leanstore/storage/KeyValueInterface.hpp>
#include "btreeNode.hpp"

using namespace leanstore::storage;

// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace lsmTree
{
class BTree : public KeyValueInterface {
  private:
   // ability to split nodes when they are full
   void splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength, unsigned payloadLength);

   // check whether affected parent nodes have enough space
   void ensureSpace(BTreeNode* toSplit, uint8_t* key, unsigned keyLength, unsigned payloadLength);

  public:
   // enable building and destroying BTrees
   BTree();
   ~BTree();

   // marker for the root node
   BTreeNode* root;

   // number of pages of a BTree, used for leveling and merging BTrees
   unsigned pageCount = 0;

   // enable point lookup
   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut);
   bool lookup(uint8_t* key, unsigned keyLength);

   // enable insertion of keys with payload
   void insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);

   // enable removing keys
   bool remove(uint8_t* key, unsigned keyLength);

   // TODO:
   // rangescan(uint8_t* lowerKey, uint8_t* upperKey)
   // update(uint8_t* key, unsigned keyLength, uint8_t* payload)
   void insertLeafSorted(uint8_t* key, unsigned int keyLength, BTreeNode* leaf);

   // TODO for LeanStore:
   // int64_t iterateAllPages(std::function<int64_t(BTreeNode&)> inner, std::function<int64_t(BTreeNode&)> leaf);
   // int64_t iterateAllPagesRec(HybridPageGuard<BTreeNode>& node_guard, std::function<int64_t(BTreeNode&)> inner, std::function<int64_t(BTreeNode&)> leaf); static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   /*struct DataTypeRegistry::DTMeta BTree::getMeta()
   {
      DataTypeRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
          .find_parent = findParent,
          .check_space_utilization = checkSpaceUtilization};
      return btree_meta;
   }*/
   static DataTypeRegistry::DTMeta getMeta();
   static bool checkSpaceUtilization(void* btree_object, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
   // static ParentSwipHandler findParent(BTree& btree_object, BufferFrame& to_find);
   static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   static void iterateChildrenSwips(void* btree_object, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback);


   //from KeyValueInterface.hpp, has to be implemented? Maybe instead inherit from BTreeGeneric?
   virtual OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   virtual OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>) override;
   virtual OP_RESULT remove(u8* key, u16 key_length) override;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                             function<void()>) override;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                              function<void()>) override;
   virtual u64 countPages() override;
   virtual u64 countEntries() override;
   virtual u64 getHeight() override;


};

}
}
}

#endif  // BTREE_LSMTREE_BTREE_HPP
