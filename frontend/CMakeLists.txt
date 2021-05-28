add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore)

add_executable(tpcc tpc-c/tpcc.cpp)
target_link_libraries(tpcc leanstore Threads::Threads)
target_include_directories(tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(rocksdb_tpcc tpc-c/rocksdb_tpcc.cpp tpc-c/TPCCWorkload.hpp)
target_link_libraries(rocksdb_tpcc leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy)
target_include_directories(rocksdb_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(rocksdb_tpcc PUBLIC ROCKSDB_ADAPTER)
# sudo apt-get install libsnappy-dev  zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev