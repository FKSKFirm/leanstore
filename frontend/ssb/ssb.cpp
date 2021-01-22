/**
 * @file ssb.cpp
 * @brief Manages the execution of the ssb workload.
 *
 */

#include "../shared/adapter-leanstore.hpp"
#include "../shared/types.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
#include "schema.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <iostream>
#include <set>
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ssb_scale_factor, 1, "");
DEFINE_int32(ssb_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(ssb_warehouse_affinity, false, "");
DEFINE_bool(ssb_fast_load, false, "");
DEFINE_bool(ssb_remove, true, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
LeanStoreAdapter<lineorder_t> lineorder_table;
LeanStoreAdapter<part_t> part_table;
LeanStoreAdapter<supplier_t> supplier_table;
LeanStoreAdapter<customer_t> customer_table;
LeanStoreAdapter<date_t> date_table;
// -------------------------------------------------------------------------------------
#include "workload.hpp"
// -------------------------------------------------------------------------------------
void setupSSB(LeanStore& db);
void loadDB(LeanStore& db);
void loadSimpleData();
void loadComplexData(std::atomic<u32>& g_w_id);
void printLoadStatistics(LeanStore& db);
void runTPCC(LeanStore& db);
void startTpccThreads(LeanStore& db, atomic<u64>& keep_running, atomic<u64>& running_threads_counter, u64 tx_per_thread[]);
void executeOneTpccTx(u64 t_i, volatile u64& tx_acc);
void stopTpccThreads(LeanStore& db, atomic<u64>& keep_running, atomic<u64>& running_threads_counter);
void printTpccStatistics(u64 tx_per_thread[]);
void printRunStatistics(LeanStore& db);

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   LeanStore db;
   setupSSB(db);
   loadDB(db);
   /*
   sleep(2);
   printLoadStatistics(db);
   runTPCC(db);
   printRunStatistics(db);

   if (FLAGS_persist) {
      db.getBufferManager().writeAllBufferFrames();
   }
   //*/
   return 0;
}

double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}

void setupSSB(LeanStore& db)
{
   auto& crm = db.getCRManager();
   scale_factor = FLAGS_ssb_scale_factor;
   crm.scheduleJobSync(0, [&]() {
      lineorder_table = LeanStoreAdapter<lineorder_t>(db, "lineorder");
      part_table = LeanStoreAdapter<part_t>(db, "part");
      supplier_table = LeanStoreAdapter<supplier_t>(db, "supplier");
      customer_table = LeanStoreAdapter<customer_t>(db, "customer");
      date_table = LeanStoreAdapter<date_t>(db, "date");
   });
   db.registerConfigEntry("ssb_scale_factor", FLAGS_ssb_scale_factor);
   db.registerConfigEntry("ssb_warehouse_affinity", FLAGS_ssb_warehouse_affinity);
   db.registerConfigEntry("run_until_tx", FLAGS_run_until_tx);
   // const u64 load_threads = (FLAGS_ssb_fast_load) ? thread::hardware_concurrency() : FLAGS_worker_threads;
}

void loadDB(LeanStore& db)
{
   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, *loadSimpleData);
   std::atomic<u32> global_scale_factor = 1;
   // crm.scheduleJobs(FLAGS_worker_threads, [&]() { loadComplexData(global_scale_factor); });
   // crm.joinAll();
}

void loadSimpleData()
{
   cr::Worker::my().startTX();
   loadDate();
   cr::Worker::my().commitTX();
}

void loadComplexData(std::atomic<u32>& global_scale_factor)
{
   while (true) {
      u32 scale_fragment = global_scale_factor++;
      if (scale_fragment > FLAGS_ssb_scale_factor) {
         return;
      }
      cr::Worker::my().startTX();
      /*
      loadPart(scale_fragment);
      loadCustomer(scale_fragment);
      loadSupplier(scale_fragment);
      loadLineOrder(scale_fragment);
      */
      cr::Worker::my().commitTX();
   }
}

void printLoadStatistics(LeanStore& db)
{
   /*
   auto& crm = db.getCRManager();
   double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << "data loaded - consumed space in GiB = " << gib << endl;
   crm.scheduleJobSync(0, [&]() { cout << "Warehouse pages = " << warehouse.btree->countPages() << endl; });
   */
}

void runTPCC(LeanStore& db)
{
   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_ssb_scale_factor, FLAGS_zipf_factor);
   u64 tx_per_thread[FLAGS_worker_threads];
   startTpccThreads(db, keep_running, running_threads_counter, tx_per_thread);
   stopTpccThreads(db, keep_running, running_threads_counter);
   printTpccStatistics(tx_per_thread);
}

void startTpccThreads(LeanStore& db, atomic<u64>& keep_running, atomic<u64>& running_threads_counter, u64 tx_per_thread[])
{
   auto& crm = db.getCRManager();
   db.startProfilingThread();
   crm.scheduleJobs(FLAGS_worker_threads, [&, tx_per_thread](u64 t_i) {
      running_threads_counter++;
      volatile u64 tx_acc = 0;
      cr::Worker::my().refreshSnapshot();
      while (keep_running) {
         jumpmuTry() { executeOneTpccTx(t_i, tx_acc); }
         jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
      }
      tx_per_thread[t_i] = tx_acc;
      running_threads_counter--;
   });
}

void executeOneTpccTx(u64 t_i, volatile u64& tx_acc)
{
   /*
   cr::Worker::my().startTX();
   u32 w_id;
   if (FLAGS_ssb_warehouse_affinity) {
      w_id = t_i + 1;
   } else {
      w_id = uRand(1, FLAGS_ssb_scale_factor);
   }
   tx(w_id);
   if (FLAGS_ssb_abort_pct && uRand(0, 100) <= FLAGS_ssb_abort_pct) {
      cr::Worker::my().abortTX();
   } else {
      cr::Worker::my().commitTX();
   }
   WorkerCounters::myCounters().tx++;
   tx_acc++;
   */
}

void stopTpccThreads(LeanStore& db, atomic<u64>& keep_running, atomic<u64>& running_threads_counter)
{
   auto& crm = db.getCRManager();
   if (FLAGS_run_until_tx) {
      while (db.getGlobalStats().accumulated_tx_counter < FLAGS_run_until_tx) {
         usleep(500);
      }
      cout << FLAGS_run_until_tx << " has been reached";
   } else {
      sleep(FLAGS_run_for_seconds);
   }
   // Shutdown threads
   keep_running = false;
   while (running_threads_counter) {
      MYPAUSE();
   }
   crm.joinAll();
}

void printTpccStatistics(u64 tx_per_thread[])
{
   cout << endl;
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      cout << tx_per_thread[t_i] << ",";
   }
   cout << endl;
}

void printRunStatistics(LeanStore& db)
{
   double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << endl << "consumed space in GiB = " << gib << endl;
}
