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
    : worker_id(worker_id), all_workers(all_workers), workers_count(workers_count), ssd_fd(fd)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   std::memset(wal_buffer, 0, WORKER_WAL_SIZE);
   my_snapshot = make_unique<u64[]>(workers_count);
   lower_water_marks = static_cast<atomic<u64>*>(std::aligned_alloc(64, 8 * sizeof(u64) * workers_count));
   for (u64 w = 0; w < workers_count; w++) {
      lower_water_marks[w * 8] = 0;
   }
}
Worker::~Worker()
{
   std::free(lower_water_marks);
   // static std::mutex m;
   // std::unique_lock guard(m);
   // cout << "WorkerID = " << worker_id << endl;
   // cout << worker_id << " high = " << high_water_mark << " - low = " << lower_water_mark << " todo# " << todo_list.size() << endl;
}
// -------------------------------------------------------------------------------------
u32 Worker::walContiguousFreeSpace()
{
   return WORKER_WAL_SIZE - wal_wt_cursor;
}
// -------------------------------------------------------------------------------------
void Worker::walEnsureEnoughSpace(u32 requested_size)
{
   if (FLAGS_wal) {
   }
}
// -------------------------------------------------------------------------------------
void Worker::invalidateEntriesUntil(u64 until)
{
   if (wal_buffer_round > 0) {
      constexpr u64 INVALIDATE_LSN = std::numeric_limits<u64>::max();
      assert(wal_next_to_clean >= wal_wt_cursor);
      assert(wal_next_to_clean <= WORKER_WAL_SIZE);
      if (wal_next_to_clean < until) {
         u64 offset = wal_next_to_clean;
         while (offset < until) {
            auto entry = reinterpret_cast<WALEntry*>(wal_buffer + offset);
            DEBUG_BLOCK()
            {
               assert(offset + entry->size <= WORKER_WAL_SIZE);
               if (entry->type != WALEntry::TYPE::CARRIAGE_RETURN) {
                  entry->checkCRC();
               }
               assert(entry->lsn < INVALIDATE_LSN);
            }
            entry->lsn.store(INVALIDATE_LSN, std::memory_order_release);
            offset += entry->size;
         }
         wal_next_to_clean = offset;
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::submitDTEntry(u64 total_size)
{
   DEBUG_BLOCK() { active_dt_entry->computeCRC(); }
   std::unique_lock<std::mutex> g(worker_group_commiter_mutex);
   const u64 next_wt_cursor = wal_wt_cursor + total_size;
   wal_wt_cursor.store(next_wt_cursor, std::memory_order_release);
   wal_max_gsn.store(clock_gsn, std::memory_order_release);
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
void Worker::startTX()
{
   if (FLAGS_wal) {
      if (FLAGS_si) {
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   if (FLAGS_wal) {
   }
}
// -------------------------------------------------------------------------------------
void Worker::abortTX()
{
   if (FLAGS_wal) {
   }
   jumpmu::jump();
}
// -------------------------------------------------------------------------------------
void Worker::WALFinder::insertJumpPoint(LID LSN, WALChunk::Slot slot)
{
   std::unique_lock guard(m);
   ht[LSN] = slot;
}
// -------------------------------------------------------------------------------------
Worker::WALFinder::~WALFinder() {}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
