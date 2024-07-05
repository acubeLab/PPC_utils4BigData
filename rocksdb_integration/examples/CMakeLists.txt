add_executable(simple_example
  simple_example.cc)
target_link_libraries(simple_example
  ${ROCKSDB_LIB})

find_package(OpenMP)
if(OpenMP_CXX_FOUND)
add_executable(PPC_access
        PPC_access.cpp)
target_link_libraries(PPC_access
        PUBLIC OpenMP::OpenMP_CXX ${ROCKSDB_LIB})
endif()

add_executable(column_families_example
  column_families_example.cc)
target_link_libraries(column_families_example
  ${ROCKSDB_LIB})

add_executable(compact_files_example
  compact_files_example.cc)
target_link_libraries(compact_files_example
  ${ROCKSDB_LIB})

add_executable(c_simple_example
  c_simple_example.c)
target_link_libraries(c_simple_example
  ${ROCKSDB_LIB})

add_executable(optimistic_transaction_example
  optimistic_transaction_example.cc)
target_link_libraries(optimistic_transaction_example
  ${ROCKSDB_LIB})

add_executable(transaction_example
  transaction_example.cc)
target_link_libraries(transaction_example
  ${ROCKSDB_LIB})

add_executable(compaction_filter_example
  compaction_filter_example.cc)
target_link_libraries(compaction_filter_example
  ${ROCKSDB_LIB})

add_executable(options_file_example
  options_file_example.cc)
target_link_libraries(options_file_example
  ${ROCKSDB_LIB})

add_executable(multi_processes_example
  EXCLUDE_FROM_ALL
  multi_processes_example.cc)
target_link_libraries(multi_processes_example
  ${ROCKSDB_LIB})
