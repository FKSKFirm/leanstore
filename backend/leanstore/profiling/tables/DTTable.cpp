#include "DTTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
DTTable::DTTable(BufferManager& bm) : bm(bm) {}
// -------------------------------------------------------------------------------------
std::string DTTable::getName()
{
   return "dt";
}
// -------------------------------------------------------------------------------------
void DTTable::open()
{
   columns.emplace("key", [&](Column& col) { col << dt_id; });
   columns.emplace("dt_name", [&](Column& col) { col << dt_name; });
   columns.emplace("dt_misses_counter", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_misses_counter, dt_id); });
   columns.emplace("dt_restarts_update_same_size",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_update_same_size, dt_id); });
   columns.emplace("dt_restarts_structural_change",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_structural_change, dt_id); });
   columns.emplace("dt_restarts_read", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_read, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("dt_empty_leaf", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_empty_leaf, dt_id); });
   columns.emplace("dt_skipped_leaf", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_skipped_leaf, dt_id); });
   columns.emplace("dt_goto_page", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_goto_page, dt_id); });
   columns.emplace("dt_next_tuple", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_next_tuple, dt_id); });
   columns.emplace("dt_prev_tuple", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_prev_tuple, dt_id); });
   columns.emplace("dt_inner_page", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_inner_page, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("contention_split_succ_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::contention_split_succ_counter, dt_id); });
   columns.emplace("contention_split_fail_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::contention_split_fail_counter, dt_id); });
   columns.emplace("dt_merge_succ", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_succ, dt_id); });
   columns.emplace("dt_merge_fail", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_fail, dt_id); });
   columns.emplace("dt_merge_parent_succ",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_parent_succ, dt_id); });
   columns.emplace("dt_merge_parent_fail",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_parent_fail, dt_id); });
   columns.emplace("xmerge_partial_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::xmerge_partial_counter, dt_id); });
   columns.emplace("xmerge_full_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::xmerge_full_counter, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("lsm_merges_inMem_tier0",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::lsm_merges_inMem_tier0, dt_id); });
   columns.emplace("lsm_merges_overall",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::lsm_merges_overall, dt_id); });
   columns.emplace("lsm_createdBloomFilters",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::lsm_createdBloomFilters, dt_id); });
   columns.emplace("lsm_reusedBloomFilters",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::lsm_reusedBloomFilters, dt_id); });
}
// -------------------------------------------------------------------------------------
void DTTable::next()
{
   clear();
   for (const auto& dt : bm.getDTRegistry().dt_instances_ht) {
      dt_id = dt.first;
      dt_name = std::get<2>(dt.second);
      for (auto& c : columns) {
         c.second.generator(c.second);
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
