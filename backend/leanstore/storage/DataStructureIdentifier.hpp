#pragma once
#include "leanstore/storage/btree/core/BTreeNode.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
enum class OP_RESULT : u8 { OK = 0, NOT_FOUND = 1, DUPLICATE = 2, ABORT_TX = 3, NOT_ENOUGH_SPACE = 4, OTHER = 5 };
// -------------------------------------------------------------------------------------
// Interface
class KeyValueInterface
{
  public:
   virtual OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) = 0;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) = 0;
   virtual OP_RESULT updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>) = 0;
   virtual OP_RESULT remove(u8* key, u16 key_length) = 0;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) = 0;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>) = 0;
   // -------------------------------------------------------------------------------------
   virtual u64 countPages() = 0;
   virtual u64 countEntries() = 0;
   virtual u64 getHeight() = 0;
};
// -------------------------------------------------------------------------------------
}  // namespace storage
using Slice = std::basic_string_view<u8>;
using StringU = std::basic_string<u8>;
struct MutableSlice {
   u8* ptr;
   u64 len;
   MutableSlice(u8* ptr, u64 len) : ptr(ptr), len(len) {}
   u64 length() { return len; }
   u8* data() { return ptr; }
};
}  // namespace leanstore
