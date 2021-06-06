#include "Worker.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <stdio.h>

#include <cstdlib>
#include <fstream>
#include <mutex>
#include <sstream>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
thread_local Worker* Worker::tls_ptr = nullptr;
// -------------------------------------------------------------------------------------
Worker::Worker(u64 worker_id, Worker** all_workers, u64 workers_count, s32 fd)
    : worker_id(worker_id),
      all_workers(all_workers),
      workers_count(workers_count),
      ssd_fd(fd)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   my_snapshot = make_unique<u64[]>(workers_count);
   lower_water_marks = static_cast<atomic<u64>*>(std::aligned_alloc(64, 8 * sizeof(u64) * workers_count));
   for (u64 w = 0; w < workers_count; w++) {
      lower_water_marks[w * 8] = 0;
   }
}
Worker::~Worker()
{
   std::free(lower_water_marks);
}
// -------------------------------------------------------------------------------------
void Worker::refreshSnapshot()
{
   for (u64 w = 0; w < workers_count; w++) {
      my_snapshot[w] = all_workers[w]->high_water_mark;
      all_workers[w]->lower_water_marks[worker_id * 8].store(my_snapshot[w], std::memory_order_release);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
