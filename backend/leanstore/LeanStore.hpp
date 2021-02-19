#pragma once
#include "Config.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
//#include "leanstore/storage/lsmtree/btree.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
class LeanStore
{
   using statCallback = std::function<void(ostream&)>;
   struct GlobalStats {
      u64 accumulated_tx_counter = 0;
   };
   // -------------------------------------------------------------------------------------
  public:
   // Poor man catalog
   std::unordered_map<string, storage::btree::BTreeLL> btrees_ll;
   // TODO Add map of your data structure here
//   std::unordered_map<string, storage::lsmTree::BTree> ownBTrees;
   // -------------------------------------------------------------------------------------
   s32 ssd_fd;
   // -------------------------------------------------------------------------------------
   unique_ptr<cr::WorkerThreadManager> cr_manager;
   unique_ptr<storage::BufferManager> buffer_manager;
   // -------------------------------------------------------------------------------------
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   profiling::ConfigsTable configs_table;
   u64 config_hash = 0;
   GlobalStats global_stats;
   // -------------------------------------------------------------------------------------
  public:
   LeanStore();
   // -------------------------------------------------------------------------------------
   template <typename T>
   void registerConfigEntry(string name, T value)
   {
      configs_table.add(name, std::to_string(value));
   }
   u64 getConfigHash();
   GlobalStats getGlobalStats();
   // -------------------------------------------------------------------------------------
   storage::btree::BTreeLL& registerBTreeLL(string name);
   storage::btree::BTreeLL& retrieveBTreeLL(string name) { return btrees_ll[name]; }
   // TODO Add your register method for your data structure
//   storage::lsmTree::BTree& registerOwnBTree(string name);
//   storage::lsmTree::BTree& retrieveOwnBTree(string name) { return ownBTrees[name]; }
   // -------------------------------------------------------------------------------------
   storage::BufferManager& getBufferManager() { return *buffer_manager; }
   cr::WorkerThreadManager& getCRManager() { return *cr_manager; }
   // -------------------------------------------------------------------------------------
   void startProfilingThread();
   void persist();
   void restore();
   // -------------------------------------------------------------------------------------
   ~LeanStore();
};
// -------------------------------------------------------------------------------------
}  // namespace leanstore
