#pragma once
#include <Units.hpp>

namespace leanstore
{
namespace storage
{
// BTreeNode should not be used, BTreeNodes have also the Type BTree (when they belong to a btree on disk) or InMemoryBTree, when they belong to the LSM inMemTree
enum class LSM_TYPE : u8 { BTree = 1, InMemoryBTree = 2, BTreeNode = 3, BloomFilter = 4, NotDefined = 0 };
// Interface
class DataStructureIdentifier
{
  public:
   // type: 0=undefined, 1=BTree, ...
   LSM_TYPE type = LSM_TYPE::NotDefined;
   // level: 0=in-Memory/Level0 on disk, 1=Level 1 on disk, ...
   u64 level = 0;
};
}  // namespace leanstore
}