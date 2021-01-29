#include "BTree.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::storage::btree::OP_RESULT;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
void BTree::undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts)
{
   if (FLAGS_vw) {
   } else if (FLAGS_vi) {
   } else {
      ensure(false);
   }
}
// ------------------------------------------------------------------------------------
void BTree::todo(void* btree_object, const u8* wal_entry_ptr, const u64 tts)
{
   if (FLAGS_vw) {
   } else if (FLAGS_vi) {
      ensure(false);
   } else {
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
