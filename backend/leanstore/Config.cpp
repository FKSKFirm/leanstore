// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_string(free_pages_list_path, "leanstore_free_pages", "");
// -------------------------------------------------------------------------------------
DEFINE_double(dram_gib, 1, "Available RAM capacity for Buffer manager");
DEFINE_double(ssd_gib, 1700, "Available capacity for SSD operations");
DEFINE_uint32(cool_pct, 10, "Start cooling pages when <= x% are free");
DEFINE_uint32(free_pct, 1, "pct");
DEFINE_uint32(partition_bits, 6, "bits per partition");
DEFINE_uint32(pp_threads, 1, "number of page provider threads");
// -------------------------------------------------------------------------------------
DEFINE_string(csv_path, "./log", "");
DEFINE_bool(csv_truncate, false, "");
DEFINE_string(ssd_path, "./leanstore", "Location of file on SSD");
DEFINE_uint32(async_batch_size, 256, "");
DEFINE_bool(trunc, false, "Truncate file");
DEFINE_uint32(falloc, 0, "Preallocate GiB");
// -------------------------------------------------------------------------------------
DEFINE_bool(print_debug, true, "");
DEFINE_bool(print_tx_console, true, "");
DEFINE_uint32(print_debug_interval_s, 1, "");
DEFINE_bool(profiling, false, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(worker_threads, 4, "");
DEFINE_bool(pin_threads, false, "Responsibility of the driver");
DEFINE_bool(smt, true, "Simultaneous multithreading");
// -------------------------------------------------------------------------------------
DEFINE_bool(root, false, "does this process have root rights ?");
// -------------------------------------------------------------------------------------
DEFINE_uint64(backoff_strategy, 0, "");
// -------------------------------------------------------------------------------------
DEFINE_string(zipf_path, "/bulk/zipf", "");
DEFINE_double(zipf_factor, 0.0, "");
DEFINE_double(target_gib, 0.0, "size of dataset in gib (exact interpretation depends on the driver)");
DEFINE_uint64(run_for_seconds, 10, "Keep the experiment running for x seconds");
DEFINE_uint64(warmup_for_seconds, 10, "Warmup for x seconds");
// -------------------------------------------------------------------------------------
DEFINE_bool(contention_split, false, "");
DEFINE_uint64(cm_update_on, 7, "as exponent of 2");
DEFINE_uint64(cm_period, 14, "as exponent of 2");
DEFINE_uint64(cm_slowpath_threshold, 1, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(xmerge, false, "");
DEFINE_uint64(xmerge_k, 5, "");
DEFINE_double(xmerge_target_pct, 80, "");
// -------------------------------------------------------------------------------------
DEFINE_uint64(backoff, 512, "");
// -------------------------------------------------------------------------------------
DEFINE_uint64(x, 512, "");
DEFINE_uint64(y, 100, "");
DEFINE_double(d, 0.0, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(bulk_insert, false, "");
// -------------------------------------------------------------------------------------
DEFINE_int64(trace_dt_id, -1, "Print a stack trace for page reads for this DT ID");
DEFINE_int64(trace_trigger_probability, 100, "");
// -------------------------------------------------------------------------------------
DEFINE_string(tag, "", "Unique identifier for this, will be appended to each line csv");
// -------------------------------------------------------------------------------------
DEFINE_bool(out_of_place, false, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(persist, false, "");
DEFINE_uint64(tmp, 0, "");
// -------------------------------------------------------------------------------------
// Main switch is lsm lsm_ variables only apply when lsm is true
DEFINE_bool(lsm, false, "Enable LSM implementation instead of BTree");
DEFINE_bool(lsm_bloomFilter, false, "Enable Bloom Filter in LSM");
DEFINE_uint64(lsm_pageSizeFactor, 10, "Factor for size of each LSM level");
DEFINE_bool(allInOneKVStore, false, "Have a single BTree/LSM-Tree for TPCC");
