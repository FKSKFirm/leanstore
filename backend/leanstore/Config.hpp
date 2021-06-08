#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DECLARE_double(dram_gib);
DECLARE_double(ssd_gib);
DECLARE_string(ssd_path);
DECLARE_uint32(worker_threads);
DECLARE_bool(pin_threads);
DECLARE_bool(smt);
DECLARE_string(csv_path);
DECLARE_bool(csv_truncate);
DECLARE_string(free_pages_list_path);
DECLARE_uint32(cool_pct);
DECLARE_uint32(free_pct);
DECLARE_uint32(partition_bits);
DECLARE_uint32(async_batch_size);
DECLARE_uint32(falloc);
DECLARE_uint32(pp_threads);
DECLARE_bool(trunc);
DECLARE_bool(fs);
DECLARE_bool(root);
DECLARE_bool(print_debug);
DECLARE_bool(print_tx_console);
DECLARE_bool(profiling);
DECLARE_uint32(print_debug_interval_s);
// -------------------------------------------------------------------------------------
DECLARE_bool(contention_split);
DECLARE_uint64(cm_update_on);
DECLARE_uint64(cm_period);
DECLARE_uint64(cm_slowpath_threshold);
// -------------------------------------------------------------------------------------
DECLARE_bool(xmerge);
DECLARE_uint64(xmerge_k);
DECLARE_double(xmerge_target_pct);
// -------------------------------------------------------------------------------------
DECLARE_string(zipf_path);
DECLARE_double(zipf_factor);
DECLARE_double(target_gib);
DECLARE_uint64(run_for_seconds);
DECLARE_uint64(warmup_for_seconds);
// -------------------------------------------------------------------------------------
DECLARE_uint64(backoff_strategy);
// -------------------------------------------------------------------------------------
DECLARE_uint64(backoff);
// -------------------------------------------------------------------------------------
DECLARE_bool(bstar);
DECLARE_bool(bulk_insert);
// -------------------------------------------------------------------------------------
DECLARE_int64(trace_dt_id);
DECLARE_int64(trace_trigger_probability);
DECLARE_string(tag);
// -------------------------------------------------------------------------------------
DECLARE_bool(out_of_place);
// -------------------------------------------------------------------------------------
DECLARE_bool(persist);
DECLARE_uint64(tmp);
// -------------------------------------------------------------------------------------
DECLARE_bool(lsm);
DECLARE_bool(lsm_bloomFilter);
DECLARE_uint64(lsm_tieringFactor);
DECLARE_bool(allInOneKVStore);
