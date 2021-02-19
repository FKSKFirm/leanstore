#pragma once
#include "core/BTreeGeneric.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/KeyValueInterface.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
class BTreeLL : public KeyValueInterface, public BTreeGeneric
{
  public:
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
   // -------------------------------------------------------------------------------------
   virtual u64 countPages() override;
   virtual u64 countEntries() override;
   virtual u64 getHeight() override;
   // -------------------------------------------------------------------------------------
   static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   static DataTypeRegistry::DTMeta getMeta();
};
// -------------------------------------------------------------------------------------
}  // namespace keyValueDataStore
}  // namespace storage
}  // namespace leanstore
