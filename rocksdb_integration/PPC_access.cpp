//
// Created by boffa on 18/09/23.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

const int NUM_THREAD = 16;

bool check_file_id(const std::string &file_id) {
  // check if it is SHA1 id
  if (file_id.size() != 40) {
    std::cout << "SHA1 id must be 40 characters long" << std::endl;
    return false;
  }
  if (file_id.find_first_not_of("0123456789abcdef") != size_t(-1)) {
    std::cout << "SHA1 id must contain just 0123456789abcdef chars"
              << std::endl;
    return false;
  }
  return true;
}

template <class T>
void do_not_optimize(T const &value) {
  asm volatile("" : : "r,m"(value) : "memory");
}

void decompress_file(const std::string &input_dir,
                     const std::string &output_dir,
                     const std::string &compressed_archive_block,
                     const std::string &file_id,
                     const std::string &decompression_script, bool INTERACTIVE,
                     bool BENCH) {
  std::string path_working_dir =
      std::filesystem::path(output_dir) /
      std::filesystem::path("tmp.Sofware_Heritage_c++_random_access_" +
                            std::to_string(getpid()));
  std::filesystem::create_directory(path_working_dir);

  std::filesystem::path cwd = std::filesystem::current_path();
  std::filesystem::current_path(path_working_dir);

  std::filesystem::path path_archive(
      std::filesystem::path(input_dir) /
      std::filesystem::path(compressed_archive_block));
  // OLD VERSION, decompress all the block
  //  std::string to_run_decompress =
  //      "tar -xf " + path_archive.string() + " -I" + decompression_script;

  std::string to_run_decompress = "tar -xf " + path_archive.string() + " " +
                                  file_id + " -I" + decompression_script;

  int status = system(to_run_decompress.c_str());
  if (status != 0) {
    std::cout << "Failed to run " << to_run_decompress << std::endl;
    exit(EXIT_FAILURE);
  }
  // bool found = false;
  //  iterate over the decompressed files
  //  If previous work, just one file has been decompressed
  //  for (const auto &entry :
  //       std::filesystem::directory_iterator(path_working_dir)) {
  //    if (entry.path().filename() == file_id) {
  //      // std::cout << "Found file! Printing it!" << std::endl;
  //      //  print the content of the file

  //      delete[] buffer;
  //
  //      found = true;
  //      file.close();
  //      break;
  //    }

  //  }

  std::filesystem::path path_file(std::filesystem::path(path_working_dir) /
                                  std::filesystem::path(file_id));

  if (not std::filesystem::exists(path_file)) {
    std::cout << "File not found! Error while decompressing file. Exiting..."
              << std::endl;
    exit(EXIT_FAILURE);
  }

  std::ifstream file(path_file);
  // get length of file
  file.seekg(0, std::ios::end);
  size_t length = file.tellg();
  file.seekg(0, std::ios::beg);

  if (INTERACTIVE) {
    // std::cout << "File content: " << std::endl;
    // std::cout << buffer << std::endl;
    try {
      std::filesystem::path out_path_file(std::filesystem::path(output_dir) /
                                          file_id);

      if (not std::filesystem::exists(out_path_file.parent_path())) {
        std::filesystem::create_directory(out_path_file.parent_path());
      }

      std::filesystem::copy_file(
          path_file, out_path_file,
          std::filesystem::copy_options::update_existing |
              std::filesystem::copy_options::recursive);

      std::cout << "OK! The desired file has been copied to " << out_path_file
                << std::endl;
    } catch (std::filesystem::filesystem_error const &ex) {
      std::cout << "ERROR! what():  " << ex.what() << std::endl
                << "path1(): " << ex.path1() << std::endl
                << "path2(): " << ex.path2() << std::endl
                << "code().value():    " << ex.code().value() << std::endl
                << "code().message():  " << ex.code().message() << std::endl
                << "code().category(): " << ex.code().category().name()
                << std::endl;
      return;
    }
  }

  if (BENCH) {
    // allocate memory
    char *buffer = new char[length];
    // read file
    file.read(buffer, length);

    size_t accumulator = 0;
    for (size_t i = 0; i < length; i++) {
      accumulator += buffer[i];
    }
    do_not_optimize(accumulator);

    delete[] buffer;
  }

  file.close();

  std::string to_run_remove = "rm -rf " + path_working_dir;
  status = system(to_run_remove.c_str());
  if (status != 0) {
    std::cout << "Failed to run " << to_run_remove << std::endl;
    exit(EXIT_FAILURE);
  }
  std::filesystem::current_path(cwd);
}

size_t decompress_files(const std::string &input_dir,
                        const std::string &output_dir, DB *const db,
                        const std::vector<std::string> &file_ids,
                        const std::string &decompression_script) {
  std::string path_working_dir =
      std::filesystem::path(output_dir) /
      std::filesystem::path("tmp.Sofware_Heritage_c++_random_access_" +
                            std::to_string(getpid()));
  std::filesystem::create_directory(path_working_dir);

  std::filesystem::path cwd = std::filesystem::current_path();
  std::filesystem::current_path(path_working_dir);
  size_t total_length = 0;

// iterate over file_ids with 16 threads
#pragma omp parallel for num_threads(NUM_THREAD)                            \
    reduction(+ : total_length) default(none)                               \
    firstprivate(input_dir, output_dir, db, file_ids, decompression_script, \
                     path_working_dir) shared(std::cout)
  for (const auto &file_id : file_ids) {
    std::string compressed_archive_block;
    Status s;
    s = db->Get(ReadOptions(), file_id, &compressed_archive_block);
    assert(s.ok());
    std::filesystem::path path_archive(
        std::filesystem::path(input_dir) /
        std::filesystem::path(compressed_archive_block));

    std::string to_run_decompress = "tar -xf ";
    to_run_decompress.append(path_archive.string());
    to_run_decompress.append(" ");
    to_run_decompress.append(file_id);
    to_run_decompress.append(" -I");
    to_run_decompress.append(decompression_script);

    int status = system(to_run_decompress.c_str());
    if (status != 0) {
      std::cout << "Exist status " << status << std::endl;
      std::cout << "In function decompress_files --> Failed to run " << to_run_decompress
                << std::endl;
      std::cout << "For file_id " << file_id << " that is in block "
                << compressed_archive_block << std::endl;
      exit(EXIT_FAILURE);
    }

    std::filesystem::path path_file(std::filesystem::path(path_working_dir) /
                                    std::filesystem::path(file_id));

    if (not std::filesystem::exists(path_file)) {
      std::cout << "File " << file_id
                << " not found! Error while decompressing file. Exiting..."
                << std::endl;
      exit(EXIT_FAILURE);
    }
    assert(std::filesystem::exists(path_file));

    std::ifstream file(path_file);
    // get length of file
    file.seekg(0, std::ios::end);
    size_t length = file.tellg();
    total_length += length;
    file.seekg(0, std::ios::beg);

    file.close();
  }

  // std::cout << "Decompressed MiB: " << (double(total_length) / double(1 <<
  // 20))
  //           << std::endl;

  int status = 0;
  std::string to_run_remove = "rm -rf " + path_working_dir;
  status = system(to_run_remove.c_str());
  if (status != 0) {
    std::cout << "Failed to run " << to_run_remove << std::endl;
    exit(EXIT_FAILURE);
  }
  std::filesystem::current_path(cwd);
  return total_length;
}

// BIG TEST
// /extralocal/swh/filename_archive_map_Python_selection_199GiB.txt zstd
// /extralocal/swh .

// SMALL TEST
// /extralocal/swh/filename_archive_map_C_small_0GiB.txt zstd /extralocal/swh/ .

int main(int argc, char **argv) {
  if (argc != 6) {
    std::cout << "Usage: " << argv[0]
              << " <archive_map_file> <decompression script> <input_dir> "
                 "<output_dir> <I/B>"
              << std::endl;
    exit(EXIT_FAILURE);
  }
  // get arguments
  std::string filename_archive_map = std::string(argv[1]);
  std::string decompression_script = std::string(argv[2]);
  std::string input_dir = std::string(argv[3]);
  std::string output_dir = std::string(argv[4]);
  std::string bench = std::string(argv[5]);

  const std::string kDBPath =
      std::filesystem::path(output_dir) /
      std::filesystem::path("rocksdb_compressed_archive_index_" +
                            std::to_string(getpid()));

  DB *db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  if (not s.ok()) {
    std::cout << "Failed to open DB" << std::endl;
    exit(EXIT_FAILURE);
  }
  bool INTERACTIVE = false;
  bool BENCH = false;
  bool TEST = false;

  if (bench == "I") {
    INTERACTIVE = true;
  }

  if (bench == "B") {
    BENCH = true;
  }

  if (bench == "T") {
    TEST = true;
  }

  size_t num_keys = 0;

  {
    std::ifstream file_archive_map(filename_archive_map);
    if (not file_archive_map.is_open()) {
      std::cout << "Failed to open file " << filename_archive_map << std::endl;
      exit(EXIT_FAILURE);
    }

    std::string line;

    while (getline(file_archive_map, line)) {
      num_keys++;
    }
  }

  std::ifstream file_archive_map(filename_archive_map);

  // Important TODO use a vector of pairs instead of two vectors
  // f the length of the the file got from the file file_archive_map
  // which will contain
  // the file_id, the SIZE, and the compressed_archive_block
  // std::vector<std::string, size_t> queries;
  std::vector<std::string> queries;
  // const size_t num_queries = std::min(size_t(100000), num_keys);
  // const size_t num_queries = std::min(size_t(100000), num_keys/20);
  // const double sample_proportion = double(num_queries) / double(num_keys);
  const double sample_proportion = 0.1;
  // std::cout << (sample_proportion);
  std::default_random_engine generator(42);
  std::bernoulli_distribution distribution(sample_proportion);

  std::string line;
  // parse file "filename_archive_map_<>.txt" line by line
  while (getline(file_archive_map, line)) {
    // find first occurrence of space in line
    size_t pos = line.find(' ');
    // file_id already contains the local_path
    std::string file_id = line.substr(0, pos);
    std::string compressed_block =
        line.substr(pos + 1, line.length() - pos - 1);
    /*if (not check_file_id(file_id)) {
      std::cout << file_id << " is not a valid file id." << std::endl;
      std::cout << "File " << filename_archive_map << " corrupted."
                << std::endl;
      exit(EXIT_FAILURE);
    }*/
    // check existance of compressed block file
    std::filesystem::path path_block_archive(
        std::filesystem::path(input_dir) /
        std::filesystem::path(compressed_block));

    if (not std::filesystem::exists(path_block_archive)) {
      std::cout << "File " << path_block_archive << " does not exist."
                << std::endl;
      exit(EXIT_FAILURE);
    }

    if (BENCH and distribution(generator)) {
      queries.push_back(file_id);
    }

    if (TEST) {
      queries.push_back(file_id);
    }

    // check if file file_id is in the tar archive compressed_block
    //    std::string to_run_tar = "tar -tf " + path_block_archive.string() + "
    //    " +
    //                             file_id + " -I" + decompression_script +
    //                             " > /dev/null";
    //    int status = system(to_run_tar.c_str());
    //    if (status != 0) {
    //      std::cout << "File " << file_id << " is not in the archive "
    //                << path_block_archive << std::endl;
    //      exit(EXIT_FAILURE);
    //    }

    s = db->Put(WriteOptions(), file_id, compressed_block);
    if (not s.ok()) {
      std::cout << "Failed to put key-value in DB" << std::endl;
      std::cout << file_id << " " << compressed_block << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  // std::cout << "Number of loaded keys: " << num_keys << ". ";

  // if (BENCH) std::cout << "Running " << queries.size() << " queries. ";

  // std::cout << std::endl << std::flush;
  //  std::string num_keys;
  //  db->GetProperty("rocksdb.estimate-num-keys", &num_keys);
  //  std::cout << "(Estimated) Number of loaded keys: " << num_keys <<
  //  std::endl;

  if (INTERACTIVE) {
    // wait for input from the user
    std::string file_sha1;
    while (true) {
      std::cout << "--> Type a FILE ID to get the corresponding file. "
                << "Type 'exit' to exit" << std::endl;

      std::getline(std::cin, file_sha1);

      if (file_sha1 == "exit") {
        break;
      }
      // check if it is SHA1 id
      /*if (not check_file_id(file_sha1)) {
        file_sha1.clear();
        continue;
      }*/
      std::string compressed_archive_block;
      //  get value
      s = db->Get(ReadOptions(), file_sha1, &compressed_archive_block);
      if (not s.ok()) {
        std::cout << "Failed to get value from DB" << std::endl;
        std::cout << "The file " << file_sha1
                  << " is not in these compressed archives" << std::endl;
        continue;
        // exit(EXIT_FAILURE);
      }
      // assert(s.ok());
      // read file named value on the filesystem
      decompress_file(input_dir, output_dir, compressed_archive_block,
                      file_sha1, decompression_script, INTERACTIVE, BENCH);
    }
  }

  //  if (BENCH) {
  //    std::shuffle(queries.begin(), queries.end(), std::mt19937(42));
  //    std::cout << "File of filename_archive_map: " << filename_archive_map
  //              << std::endl;
  //    std::cout << "Running " << queries.size() << " queries" << std::endl;
  //    auto begin = std::chrono::steady_clock::now();
  //    auto rocksdb_time = std::chrono::steady_clock::duration::zero();
  //    for (auto &q : queries) {
  //      std::string compressed_archive_block;
  //      //  get value
  //      auto begin_rocksdb = std::chrono::steady_clock::now();
  //      s = db->Get(ReadOptions(), q, &compressed_archive_block);
  //      rocksdb_time += std::chrono::steady_clock::now() - begin_rocksdb;
  //      // assert(s.ok());
  //      // read file named value on the filesystem
  //      decompress_file(input_dir, output_dir, compressed_archive_block, q,
  //                      decompression_script, INTERACTIVE, BENCH);
  //    }
  //    auto end = std::chrono::steady_clock::now();
  //    std::cout << "TOTAL time milliseconds/Query: "
  //              << (std::chrono::duration_cast<std::chrono::milliseconds>(end
  //              -
  //                                                                        begin)
  //                      .count() /
  //                  double(queries.size()))
  //              << std::endl;
  //    std::cout << "RocksDB time milliseconds/Query: "
  //              << (std::chrono::duration_cast<std::chrono::milliseconds>(
  //                      rocksdb_time)
  //                      .count() /
  //                  double(queries.size()))
  //              << std::endl;
  //  }
  if (BENCH) {
    std::shuffle(queries.begin(), queries.end(), std::mt19937(42));
    // std::cout << "File of filename_archive_map: " << filename_archive_map
    //           << std::endl;
    auto begin = std::chrono::steady_clock::now();
    size_t total_length = decompress_files(input_dir, output_dir, db, queries,
                                           decompression_script);
    auto end = std::chrono::steady_clock::now();
    auto elapsed_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
            .count();
    // auto elapsed_time_seconds =
    //     std::chrono::duration_cast<std::chrono::seconds>(end -
    //     begin).count();
    std::cout << "FILE_ARCHIVE_MAP,TIME_FILE_DECOMPRESSION(ms/file),"
                 "FILE_ACCESS_SPEED(MiB/s),THROUGHPUT(files/s)"
              << std::endl;
    std::cout << filename_archive_map.substr(
                     filename_archive_map.find_last_of("/\\") + 1)
              << ",";
    std::cout << std::fixed << std::showpoint;
    std::cout << std::setprecision(2);
    std::cout << double(elapsed_time) / double(queries.size()) << ",";
    std::cout << (double(total_length) / double(1 << 20)) /
                     double(elapsed_time) * 1000
              << ",";
    std::cout << (double(queries.size()) / double(elapsed_time)) * 1000
              << std::endl;
  }

  if (TEST) {
    std::cout << "Number of loaded keys: " << num_keys << ". ";
    std::cout << "Running " << queries.size() << " queries. ";

    std::cout << "Testing if all the files in the filename_archive_map: "
              << filename_archive_map.substr(
                     filename_archive_map.find_last_of("/\\") + 1)
              << " are singularly decompressable." << std::endl;

    decompress_files(input_dir, output_dir, db, queries, decompression_script);

    std::cout << "ALL the files in: "
              << filename_archive_map.substr(
                     filename_archive_map.find_last_of("/\\") + 1)
              << " are singularly decompressable!" << std::endl;
  }

  delete db;
  std::string to_run_remove = "rm -rf " + kDBPath;
  int status = system(to_run_remove.c_str());
  if (status != 0) {
    std::cout << "Failed to run " << to_run_remove << std::endl;
    exit(EXIT_FAILURE);
  }

  return EXIT_SUCCESS;
}
