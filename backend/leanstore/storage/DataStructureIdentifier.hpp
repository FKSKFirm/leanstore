#pragma once
#include <Units.hpp>

namespace leanstore
{
namespace storage
{
enum class LSM_TYPE : u8 { BTree = 0, BloomFilter = 1, NotDefined = 2 };
// Interface
class DataStructureIdentifier
{
  public:
   // type: 0=btree, 1=bloomFilter
   LSM_TYPE type = LSM_TYPE::NotDefined;
   // level: 0=in-Memory, 1=Level 1 on disk, ...
   u64 level = 0;
};
}  // namespace leanstore
}