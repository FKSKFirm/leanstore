#pragma once
#include "Config.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/btree/BTreeVI.hpp"
#include "storage/btree/BTreeVW.hpp"
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
   std::unordered_map<string, storage::btree::BTreeVW> btrees_vw;
   std::unordered_map<string, storage::btree::BTreeVI> btrees_vi;
   // -------------------------------------------------------------------------------------
   s32 ssd_fd;
   // -------------------------------------------------------------------------------------
   unique_ptr<cr::CRManager> cr_manager;
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
   storage::btree::BTreeVW& registerBTreeVW(string name);
   storage::btree::BTreeVW& retrieveBTreeVW(string name) { return btrees_vw[name]; }
   storage::btree::BTreeVI& registerBTreeVI(string name);
   storage::btree::BTreeVI& retrieveBTreeVI(string name) { return btrees_vi[name]; }
   // -------------------------------------------------------------------------------------
   storage::BufferManager& getBufferManager() { return *buffer_manager; }
   cr::CRManager& getCRManager() { return *cr_manager; }
   // -------------------------------------------------------------------------------------
   void startProfilingThread();
   void persist();
   void restore();
   // -------------------------------------------------------------------------------------
   ~LeanStore();
};
// -------------------------------------------------------------------------------------
}  // namespace leanstore
