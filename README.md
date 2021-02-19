# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling

Install dependencies:

`sudo apt-get install cmake libaio-dev libtbb-dev`

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

`cd frontend && touch leanstore`

## TPC-C Example

`build/frontend/tpcc --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=240 --tpcc_warehouse_count=100 --notpcc_warehouse_affinity --csv_path=./log --cool_pct=40 --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60`

check `build/frontend/tpcc --help` for other options

## Structure

- backend/leanstore: Implementation of Leanstore
  - Config.cpp: Configuration of flags
  - storage:
    - keyValueDataStore: Different B-Tree Implementations
      - BTreeLL: LowLevel implementation, without concurrency control
      - BTreeSI: Snapshot Isolation
	  - BTreeSlotted: ??
      - BTreeVI: Version-In-Place
      - BTreeVW: Version in Write-Ahead-Log (WAL)
    - buffer-manager:
- frontend: Benchmarks
  - tbcc: Implementation of the [tbc-c benchmark](http://www.tpc.org/tpcc/)
  - ycsb: Implementation of the [ycsb benchmark](https://research.yahoo.com/news/yahoo-cloud-serving-benchmark/)

## Naming conventions

functionName
ClassName
ClassName.hpp
ClassName.cpp
variable_name
directory-name
file_name

## Cite

The code we used for our CIDR 2021 paper is in a different (and outdated) [branch](https://github.com/leanstore/leanstore/tree/cidr).

```
@inproceedings{alhomssi21,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Contention and Space Management in B-Trees},
    booktitle = {CIDR},
    year      = {2021}
}
```
