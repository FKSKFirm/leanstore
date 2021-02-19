#include "CRTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
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
std::string CRTable::getName()
{
   return "cr";
}
// -------------------------------------------------------------------------------------
void CRTable::open()
{
   columns.emplace("key", [&](Column& out) { out << 0; });
   columns.emplace("gct_phase_1_pct", [&](Column& col) { col << 100.0 * p1 / total; });
   columns.emplace("gct_phase_2_pct", [&](Column& col) { col << 100.0 * p2 / total; });
   columns.emplace("gct_write_pct", [&](Column& col) { col << 100.0 * write / total; });
   columns.emplace("gct_committed_tx", [&](Column& col) { col << sum(CRCounters::cr_counters, &CRCounters::gct_committed_tx); });
   columns.emplace("gct_rounds", [&](Column& col) { col << sum(CRCounters::cr_counters, &CRCounters::gct_rounds); });
   columns.emplace("tx", [](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::tx); });
   columns.emplace("tx_abort", [](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::tx_abort); });
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
void CRTable::next()
{
   // -------------------------------------------------------------------------------------
   p1 = sum(CRCounters::cr_counters, &CRCounters::gct_phase_1_ms);
   p2 = sum(CRCounters::cr_counters, &CRCounters::gct_phase_2_ms);
   write = sum(CRCounters::cr_counters, &CRCounters::gct_write_ms);
   total = p1 + p2 + write;
   clear();
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
