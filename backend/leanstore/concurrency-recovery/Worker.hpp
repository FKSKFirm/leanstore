#pragma once
#include "Transaction.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
struct Worker {
   // Static
   static thread_local Worker* tls_ptr;
   // -------------------------------------------------------------------------------------
   const u64 worker_id;
   Worker** all_workers;
   const u64 workers_count;
   const s32 ssd_fd;
   Worker(u64 worker_id, Worker** all_workers, u64 workers_count, s32 fd);
   static inline Worker& my() { return *Worker::tls_ptr; }
   ~Worker();
   atomic<u64> high_water_mark = 0;  // High water mark, exclusive: TS < mark are visible
   atomic<u64>* lower_water_marks;
   // -------------------------------------------------------------------------------------
   unique_ptr<u64[]> my_snapshot;
  private:
   // Without Payload, by submit no need to update clock (gsn)

  public:
   // -------------------------------------------------------------------------------------
   // TX Control
   void startTX();
   void commitTX();
   void abortTX();
   // -------------------------------------------------------------------------------------
   void refreshSnapshot();
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
