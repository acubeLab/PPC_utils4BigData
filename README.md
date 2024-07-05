# Utils for the Permute-Partition-Compress paradigm over Massive File Collections

## Introduction

In this project, we aim to design and implement a software library allowing to compress collections of several billions of texts and source code files (written in markup and programming languages, thus not just HTML) fully exploiting the computational power of the PPC-paradigm to achieve effective compression ratio and efficient (de)compression speed in two different scenarios: 

- Backup: we support only streaming access to the whole compressed collection;
  
- RandomAccess: we support efficient access to individual files of the compressed collection. 

We plan to test our library on the Software Heritage archive [[3]](#3) whose size is more than 1 PB of data, and it is continuously growing.

We point out that the **current library** is designed to manage collections of **source-code blobs** (as in the case of the Software Heritage archive), because its parser processes blobs line-by-line. However, its design is sufficiently general to be used to compress collections of arbitrary files, not necessarily source codes, given that its parser can be easily extended by changing the tokenizer so that it does not work on entire lines but on q-grams or other substrings properly defined (see subsection below).

## Set up environment 

Prerequisites: `python3.11 python3.11-dev python3.11-venv libfuzzy-dev zstd`. 
If you are on MAC install: `libmagic` and `gnu-tar`

- Clone this repo `git clone https://github.com/aboffa/PPC_utils4BigData.git`
- Go into the repo directory `cd PPC_utils4BigData`
- Create virtual environment `python3.11 -m venv ppc_venv`
- Enter the virtual environment `source ppc_venv/bin/activate`
- Install dependencies `pip install -r requirements.txt`
- Run `./bench_PPC.py -h` to display usage.


Some libraries (i.e. `ssdeep`, `guesslang`) used by the `ssdeepsort` and `typeminhashgraph` permuters are old and not compatible with modern python. The permuters `tlshsort` and `typemagikaminhashgraph` have same or better performance and are fully compatible with python `3.11` but still we show how to set up an environment with `python3.8` that will fully reproduce the experiments:

Prerequisites: `python3.8 python3.8-dev python3.8-venv libfuzzy-dev zstd`. 

- Clone this repo `git clone https://github.com/aboffa/PPC_utils4BigData.git`
- Go into the repo directory `cd PPC_utils4BigData`
- Create virtual environment `python3.8 -m venv ppc_venv`
- Enter the virtual environment `source ppc_venv/bin/activate`
- Install prerequisites `pip3 install --upgrade pip setuptools==44.0.0 wheel==0.41.2`
- Install dependencies `pip3 install --no-dependencies -r requirements_full.txt`
- Run `./bench_PPC.py -h` to display usage.

## Download the datasets

Run:
```bash
./download_dataset.py -s <size> -l <language> -o <output-dir>
```

To download, inside the `<output-dir>` directory, (i) a csv list of blobs and (ii) a compressed archive containing all of them. 

The available sizes are: DEBUG, 25GiB, 50GiB, 200GiB, 1TiB. The DEBUG datasets are 3 small dataset used for debugging purposes. The 50GiB dataset is made of the blobs from most common repositories on GitHub. The others are samples from the [Software Heritage Archive](https://archive.softwareheritage.org/) and are available in the following languages: C/C++, Python, Java, Javascript, and random. More info [here](#datasets-details).

For example, running:
```bash
./download_dataset.py -s DEBUG -o ./tmp
```
You will get in the `./tmp` directory 3 small datasets: `random_small` contains 500 blobs (total uncompressed size 34MiB), `Python_small` contains 3.8K blobs (total uncompressed size 75MiB), and `C_small` contains 7.6K blobs (total uncompressed size 259MiB).


Or, for example, running:
```bash
./download_dataset.py -s 25GiB -l Python -o ./tmp
```
You will get in the `./tmp` directory the archive `Python_selection_filename+path_sort_25GiB.tar.zstd_22` and the list `Python_selection_info.csv`.

Use `-s 50GiB` to download the 50GiB dataset from most popular repositories from GitHub.

If some archives are not available, the files are easly downloadable using `download_from_list.py` script.

## Decompress the datasets

To decompress the archives use the following command

```bash
./decompress_dataset.py --dataset <abs_path_archive> -o <destination_dir>
```

where `<abs_path_archive>` is the absolute path where you downloaded the archive, and `<destination_dir>` is the directory where you want to store the extracted files. Since archives can contain thousands of files, if `<destination_dir>` already exists must be empty. To force decompressing more archives into one directory use `--force`.

For example: 
```bash
./decompress_dataset.py --dataset <abs_path>/random_small_filename+path_sort_0GiB.tar.zstd_22 -o ./tmp/blobs
```
decompresses the small random dataset into the `./tmp/blobs` directory.

Or, for example: 
```bash
./decompress_dataset.py --dataset <abs_path>/Python_selection_filename+path_sort_25GiB.tar.zstd_22 -o ./tmp/blobs
```
decompresses the entire 25GiB Python dataset into the `./tmp/blobs` directory.

Once you decompressed an archive you are ready to [quick start](#quick-start).

## Decompress from list 

If you have just the list of SWH blobs, you can dowload all of them from the [official Software Heritage Bucket](https://registry.opendata.aws/software-heritage/) using the script `download_from_list.py`.
You just need to intall boto3 and smar_open (`pip install boto3 smart_open`).s

## 🧪 Quick start: 

After you [downloaded](#download-the-datasets) and [decompressed](#decompress-the-datasets) the datasets, the command:

```bash
./bench_PPC.py examples/random_small.csv -c zstd gzip -p random filename simhashsort -i <input-dir> -o <ouptput-dir>
```

permutes the blobs (that must be inside the `<input-dir>` directory, so `<input-dir>` must be the directory where you [decompressed](#decompress-the-datasets) the archive you [downloaded](#download-the-datasets)) using three different strategies: random order (random), lexicographic order (filename), and the order induced by sorting the simhash fingerprint adapted to work on source code files (simhashsort). 

For each permutation it concatenates the permuted blobs using `tar` and the result is then compressed using `zstd` and `gzip` (storing temporary archives in `<ouptput-dir>` directory). The output of the script are the following lines showing some characteristics of the dataset and the performance in terms of compression ratio, compression speed, decompression speed, etc... of the six possible combinations between permutation and compression algorithms. 

| DATASET                | NUM_FILES | TOTAL_SIZE(GiB) | AVG_FILE_SIZE(KiB) | MEDIAN_FILE_SIZE(KiB) | TECHNIQUE            | COMPRESSION_RATIO(%) | COMPRESSION_SPEED(MiB/s) | ORDERING_TIME(s) | COMPRESSION_TIME(s) | DECOMPRESSION_SPEED(MiB/s) |
| ---------------------- | --------- | --------------- | ------------------ | --------------------- | -------------------- | -------------------- | ------------------------ | ---------------- | ------------------- | -------------------------- |
| random_small | 500       | 0.03            | 68.0               | 4.65                  | random_order+zstd | 21.848               | 134.97                   | 0.0              | 0.25                | 165.64                     |
| random_small | 500       | 0.03            | 68.0               | 4.65                  | filename_sort+zstd   | 21.559               | 176.28                   | 0.0              | 0.18                | 92.65                      |
| random_small | 500       | 0.03            | 68.0               | 4.65                  | simhash_sort+zstd    | 21.734               | 11.95                    | 2.59             | 0.19                | 200.68                     |
| random_small | 500       | 0.03            | 68.0               | 4.65                  | random_order+gzip    | 20.979               | 40.0                     | 0.0              | 0.83                | 167.37                     |
| random_small | 500       | 0.03            | 68.0               | 4.65                  | filename_sort+gzip   | 20.827               | 39.75                    | 0.0              | 0.83                | 157.59                     |
| random_small | 500       | 0.03            | 68.0               | 4.65                  | simhash_sort+gzip    | 20.939               | 9.39                     | 2.07             | 0.84                | 171.0                      |
|                        |           |                 |                    |                       |                      |                      |                          |                  |                     |                            |


More precisely, the script `bench_PPC.py` takes as input a list of files (contained in a CSV file), permutes them according to one or more techniques (`-p` option), concatenates them and optionally splits the concatenation into blocks (`-b` option). Finally, it compresses each block using one or more compressors (`-c` option).
The script measures and displays several numbers: such as compression ratio, compression speed, decompression speed, etc... 

The precise usage is:

```
usage: bench_PPC.py [-h] [-c COMPRESSOR [COMPRESSOR ...]] [-p PERM [PERM ...]] [-b BLOCK_SIZE [BLOCK_SIZE ...]] [-i INPUT_DIR] [-o OUTPUT_DIR] [-k] [-m] [-s] [--type-stats]
                    [-T NUM_THREAD] [-v] [-V]
                    csv-file [csv-file ...]

Permute-Partition-Compress paradigm on large file collections

Take as input a list of files (csv-file parameters), permute them 
according to one or more techniques (-p option), concatenate them and 
optionally split the concatenation in blocks (-b option), and finally
compress each block using one or more compressors (-c option).

The input files must be in the same directory (-i option). Temporary files
and compressed archives are stored in a user-provided directory (-o option)

Finally the archives are decompressed; the compression ratio and compression 
and decompression speed are reported on stdin.

positional arguments:
  csv-file              List of files to compress (in csv format)

optional arguments:
  -h, --help            show this help message and exit
  -c COMPRESSOR [COMPRESSOR ...], --compressor COMPRESSOR [COMPRESSOR ...]
                        Compressors to apply to each block, default: zstd
                        See doc for how to pass options to a compressor
  -p PERM [PERM ...], --permuter PERM [PERM ...]
                        Permutation strategies, one or more of the following:
                        * random: Permute blobs randomly
                        * lengthsort: Sort blobs according to legth
                        * list: No permutation, just use the order in the csv list
                        * filename: Sort blobs according to filename
                        * filename-path: Sort blobs by filename and path
                        * tlshsort: Sort blobs by TLSH
                        * ssdeepsort: Sort blobs by ssdeep
                        * simhashsort: Sort blobs by simhash
                        * minhashgraph: Sort blobs by minhash graph
                        * typeminhashgraph: Group by type(mime+lang)
                          and then by minhash-graph on the individual groups
                        * typemagikaminhashgraph: Group by type(magika library)
                          and then by minhash-graph on the individual groups
                        * all: Run all the permuting algorithms above
  -b BLOCK_SIZE [BLOCK_SIZE ...], --block-size BLOCK_SIZE [BLOCK_SIZE ...]
                        If 0 a single archive is created. Otherwise, blocks
                        of BLOCK_SIZE bytes are created before compression.
                        BLOCK_SIZE must be an integer followed by an unit
                        denoting a power of 1024. Examples: -b 512KiB -b 1MiB
                        Valid units are: KiB, MiB, GiB. Default: 0
  -i INPUT_DIR, --input-dir INPUT_DIR
                        Directory where the uncompressed blobs are storeddefault: /data/swh/blobs_uncompressed
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Directory for temporary files and compressed archivesdefault: /extralocal/swh/
  -k, --keep-tar        Keep tar archives after benchmark. The resulting
                        tar archives are stored in the `--output-dir` directory
  -m, --mmap            Use mmap on data. The blobs must be concatenated in a single `*_big_archive` file
                        See the function `create_big_archive` in mmap_on_compressed_data.py
  -s, --stats           Just print stats of the dataset, no benchmark is performed
  --type-stats          Print stats about the type of the blobs of the dataset, no benchmark is performed
  -T NUM_THREAD, --num-thread NUM_THREAD
                        Number of thread used for the compress blocks in parallell, default: 16
  -v, --verbose         Print verbose output
  -V, --version         Print version and exit
```

If the `--keep-tar` flag is set, the resulting tar archive is stored in the `--output-dir` directory.

Adding the option `-b BLOCK_SIZE` the script partitions the blobs into blocks of size `BLOCK_SIZE` bytes to enable random access to the compressed blobs. The `BLOCK_SIZE` argument is an integer and optional unit (For example: 10KiB is 10*1024 bytes). Example of block sizes: `-b 512KiB` or `-b 1MiB`. Units are KiB, MiB, GiB.

If the`--keep-tar` flag is used with the option `-b BLOCK_SIZE`, the script partitions the files into blocks of size `BLOCK_SIZE` bytes, and the resulting tar archives are stored in the `--output-dir` directory. In the same directory, the script also creates the file `filename_archive_map.txt` containing the information about which archive contains which file. More precisely, `filename_archive_map.txt` contains, for each file, a line `<file X SHA1> <archive in which file X is stored compressed>`. This file is useful if you want to "index", so access without a full decompression, the compressed archives. How to do that is explained in [🔺 Indexing the compressed archives](#🔺-indexing-the-compressed-archives)

## 🔺 Indexing the compressed archives

The script `bench_PPC.py` with the flags `-k (--keep-tar)` and `-b BLOCK_SIZE` stores in the `output-dir` directory a set of tar archives (compressed). Each one of these archives stores at most `BLOCK_SIZE` bytes (uncompressed).

In the random access scenario our main goal is to offer the possibility of getting one particular file uncompressed without decompressing all the others in a reasonably small amount of time. 

In order to retrieve a particular file we need to "index" the set of compressed tar archives, which means we need to create a mapping file ID --> block in which it is compressed. Once we know the exact block hosting the desired file, we just need to decompress it.

The actual mapping is stored by the script `bench_PPC.py` in the `output-dir` directory in a file named `filename_archive_map_....txt`. This file is just a sequence of lines:

```
<local path/file X SHA1> <archive in which file X is stored compressed>
<local path/file Y SHA1> <archive in which file Y is stored compressed>
...
```

In order to query this mapping we load the pairs `<local path/SHA1 of file X, compressed archive in which file X is>` into the popular and efficient key-value store engine [RocksDB](https://github.com/facebook/rocksdb).

To compile the `PPC_access.cpp`, download the original [RockDB](https://github.com/facebook/rocksdb) repository and move into it:

```
git clone https://github.com/facebook/rocksdb
cd rocksdb
git checkout 47e023abbd2db5f715dde923af84b37b5b05c039
```

Copy the `CMakeLists.txt` and the `PPC_access.cpp` files in the `rocksdb_integration` directory to the just downloaded RocksDB repo. Do it in the following way:

```bash
cp <PATH_SOFTWARE-HERITAGE_INSTALLATION>/rocksdb_integration/PPC_access.cpp ./examples/
cp <PATH_SOFTWARE-HERITAGETHIS_INSTALLATION>/rocksdb_integration/examples/CMakeLists.txt ./examples/
cp <PATH_SOFTWARE-HERITAGE_INSTALLATION>/rocksdb_integration/CMakeLists.txt .
```

Then compile and run:

```bash
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
cd examples
./PPC_access <archive_map_file> <decompression script> <input_dir> <output_dir> <I/B>
```

The executable `PPC_access` takes 5 arguments, the first is the path of the archive map file (`filename_archive_map_....txt` as described above), the second is the compression/decompression script used by `bench_PPC.py` (more info [here](../README.md#how-to-define-a-custom-compressor)), the third is the directory in which the compressed archives are, the forth is the directory in which the requested files will be decompressed, and the fifth and last can be `I` or `B`. 

For example, you can run:
```bash
./PPC_access /extralocal/swh/DEBUG_TEST_BLOCK_COMPRESSED_ROCKSDB/filename_archive_map_Javascript_selection_filename_sort_25GiB_2MiB.txt /home/boffa PPC_utils4BigData/compressor_flags/zstd_12_T16 /extralocal/swh/DEBUG_TEST_BLOCK_COMPRESSED_ROCKSDB /tmp I
```

`I` stands for `INTERACTIVE` mode and `B` stands for `BENCH` mode. Indeed, the executable `PPC_access` can work in two "modes", `INTERACTIVE` and `BENCH`. 

In `INTERACTIVE` mode, the program will ask you to type the SHA1 of a file (precisely it will ask: "Type a FILE ID to get the corresponding file."). Then the program will get the archive in which the file is compressed from RocksDB, will decompress the file and copy it in the `<output_dir>` directory.

For example an interaction with the program can look like:
```bash
--> Type a FILE ID to get the corresponding file. Type 'exit' to exit
asejnfcjwangnerauicgn // RANDOM STRING (TO TEST ERROR HANDLING)
ERROR! SHA1 id must be 40 characters long

--> Type a FILE ID to get the corresponding file. Type 'exit' to exit
0a0b46a0950cbc7d2cfd3a98fe058ac3ae6a4f2X // STRING LIKE A SHA1 BUT WITH AN ENDING 'X' (TO TEST ERROR HANDLING)
ERROR! SHA1 id must contain just 0123456789abcdef chars

--> Type a FILE ID to get the corresponding file. Type 'exit' to exit
fff0e59fb2652833077bff6238e8996bc47ff171 // SHA1 not in the compressed archives (TO TEST ERROR HANDLING)
ERROR! Failed to get value from RocksDB
The file fff0e59fb2652833077bff6238e8996bc47ff171 is not in this compressed archive

--> Type a FILE ID to get the corresponding file. Type 'exit' to exit
cd/cd91562ab5b39d8ff5f5e83f8836159ab13f3d88 // CORRECT SHA1
OK! The desired file has been copied to "/tmp/cd/cd91562ab5b39d8ff5f5e83f8836159ab13f3d88"

--> Type a FILE ID to get the corresponding file. Type 'exit' to exit
exit

Process finished with exit code 0
```

In `BENCH` mode, the program will benchmark the speed to decompress a file. This is done by selecting 5% (`double sample_proportion = 0.05;`) of the files in the archives at random, and the time to get these files decompressed will be measured. The output of the program will be the amount of milliseconds that are needed to decompress a file, on average.

For example the output of the BENCH mode can look like this: 

```
Number of loaded keys: 434681. Running 43472 queries. File of filename_archive_map: /extralocal/swh/DEBUG_TEST_BLOCK_COMPRESSED_ROCKSDB/filename_archive_map_Javascript_selection_filename_sort_25GiB_2MiB.txt
TIME_FILE_DECOMPRESSION(ms/file),FILE_ACCESS_SPEED(MiB/s),THROUGHPUT(blobs/s)
1.03,56.56,988.00

```

## Customizing the library 

### How to define a custom compressor 

Instead of the `zstd` other compressors installed on the machine can be used (like `gzip` or `xz`). 

But due to compatibility with `python` and `tar` (that are extensively used) after the '-c' flag only one-word commands can be used. 
So you can use `zstd`, `gzip`, `xz`, etc., but not `zstd -12` o `gzip -9`. To simplify this usage the directory `<PATH_THIS_INSTALLATION>/compressor_flags/` 
includes some of the most known flags already implemented as one-word commands:

```
  gzip_6:          gzip -6
  gzip_max:        gzip -9
  pzstd_12_p16:    pzstd -12 -p 16
  pzstd_19_p16:    pzstd -19 -p 16
  zstd_12_nl_T16:  zstd -12 --adapt -T 16 
  zstd_19_nl_T16:  zstd -19 --adapt -T 16
  zstd_19_T16:     zstd -19 -T 16 --adapt --long = 30  
  zstd_22_T16:     zstd -22 -T 16 --ultra -M1024MB --adapt --long=30
  zstd_22_T32:     zstd -22 -T 32 --ultra -M1024MB --adapt --long=30
```

where the flags for `zstd` have the following meaning: `-T` indicates the number of threads, `-M` indicates the available internal memory, 
`--long` specifies the logarithm of the length of the compression-window, `--adapt` allows the compressor to adapt the compression level to the I/O conditions, 
`--ultra` enables compression levels beyond 19.  

`pzstd` is the parallelized version of `zstd`, where the option `-p` indicates the number of parallel processes, and `-12` or `-19`
specify the compression level.

So, if you want to use one of these compressors, you can pass them to the `bench_PPC.py` script (with full path) in this way:

`./bench_PPC.py examples/random_small.csv -c <PATH_THIS_INSTALLATION>/compressor_flags/zstd_12_T16 -p random filename`

which runs `zstd_12_T16` over the files listed in `random_small.csv` according to the permuters `random` and `filename`.

To adopt other flags [this workaround](https://www.gnu.org/software/tar/manual/html_node/gzip.html) can be used. In fact `tar` has a set of predefined 
compressors (`gzip`, `bzip2`, `lzma`, `zstd`, etc...). If you want to define your own `test_compressor` with flags `--test_flag1 --test_flag2`, 
you need to create the BASH script `test_compressor.sh`:

```bash
#! /bin/sh
case $1 in
        -d) test_compressor -d --test_flag1 --test_flag2;;
        '') test_compressor --test_flag1 --test_flag2;;
        *)  echo "Unknown option $1">&2; exit 1;;
esac
```

and pass it to the corresponding option of `bench_PPC.py` script as `-cFULL_PATH_TO_SCRIPT>/test_compressor.sh`.


### How to define a custom tokenizer 

Locality-sensitive hashing techniques (like Simhash, Minhash, etc...) view each file as a sequence of tokens. Tokens can be a sequence of bytes, words, lines, or arbitrary parts of a file. 

In this project we are working with source code files. Developers, while improving the code base of their software, essentially add or delete lines to/from textual files. This is why we consider a file as a sequence of lines (i.e., divided by the `\n` char), and we feed the LSH with lines to get similar fingerprints from files that are different versions of the same source code artifact. 


Tokens could be made of groups of consecutive lines (got from a sliding window of a certain width), but we experimentally evaluated that it is better to have tokens made of single lines. We want to be robust with respect to tiny and irrelevant changes to the files, so we remove leading and trailing tabs and white spaces from the tokens (lines) and we do not consider tokens too short (<10 chars).

The code we use to tokenize the files is the following:

```python
def get_tokens(file_content, width=1, len_limit=10):
    # get a list of lines from the content of the file
    tokens = file_content.split('\n')
    if width > 1:
        # tokes are consecutive lines grouped togheter with a sliding window
        tokens = [tokens[i:i+width][0] for i in range(max(len(tokens)-width+1,1))]
    # remove tokens with less than 10 chars and delete leading and trailing tabs and whitespaces
    tokens = [x.strip() for x in tokens if len(x) > len_limit]
    return tokens
```

and it is available [here](https://github.com/aboffa/PPC_utils4BigData/blob/6e09246234968d83603c7b6c8b0f24d045ca45ae/utils.py#L147). It takes the content of a file and returns a list of tokens on which the LSH will operate. The body of the function can be easily changed in order to consider different tokens. 



## 🧪 How to run the baseline solution (files compressed individually):
- Set up environment (as described [here](#Set up environment))
- Run `./bench_single_blob.py -h` to display usage.
- As an example, run `./bench_single_blob.py examples/random_small.csv -c gzip -i <Dir where the blobs are stored>`

  
```
This script is used to measure the performance of individually compressing a set of files (in the list csv-file) with gzip (or another custom compressor).

positional arguments:
  csv-file              List of files (in CSV format) to compress

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_DIR, --input-dir INPUT_DIR
                        Directory where the blobs are stored (by default `/data/swh/blobs_uncompressed`)
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Directory used to store the temporary files (by default `/extralocal/swh/`)
  -c COMPRESSOR [COMPRESSOR ...], --compressor COMPRESSOR [COMPRESSOR ...]
                        Compressors to apply to each file, default: gzip See doc for how to pass options to a compressor
  -k, --keep            Keep the compressed files after benchmark. The resulting compressed files are stored in the `--output-dir` directory
```

## 🆕 Compress your directories!

Use `list_from_dir.py` to generate a csv file containing info about the files in your PC.
Use
```bash
./list_from_dir.py <abs-path>/directory
```
to generate a csv file compatible with `bench_PPC.py`.

For example, given these directories:
```bash
$ tree -a /home/boffa/demo
/home/boffa/demo
├── experiment
│   ├── main.c
│   └── results.csv
└── project
    ├── first_version.tex
    └── second_version.tex

2 directories, 4 files
```
The command

```bash
./list_from_dir.py /home/boffa/demo
```

generates the csv file `demo_list_of_files.csv` containing:

| swh_id | file_id            | length  | filename           | filepath                   | local_path |
|--------|--------------------|---------|--------------------|----------------------------|------------|
| 0      | first_version.tex  | 1048576 | first_version.tex  | project/first_version.tex  | project    |
| 0      | second_version.tex | 2097152 | second_version.tex | project/second_version.tex | project    |
| 0      | main.c             | 838860  | main.c             | experiment/main.c          | experiment |
| 0      | results.csv        | 1258291 | results.csv        | experiment/results.csv     | experiment |


And so you can benchmark the performance of the PPC framework on the `demo` directory with the following command:

```bash
./bench_PPC.py demo_list_of_files.csv -p all -i /home/boffa/demo
```
## Datasets details

Link with all datasets: 

https://drive.google.com/drive/folders/19mwnYOOsGkbMVq7N2VrHYMnsMxlyrh5r?usp=drive_link

### Software Heritage Archive samples

Each dataset is a subset of the Software Heritage Archive and has three different sizes: 25GiB and 200GiB.

For each size, there are subsets of the Software Heritage Project containing: 
- random content
- C/C++ source-code files (extension ['c', 'C', 'cc', 'cpp', 'CPP', 'c++', 'cp', 'cxx', 'h','hpp', 'h++', 'HPP'])
- Python source-code files (extension [‘py’, ‘pyi’])
- Java source-code files (extension [‘java’])
- Javascript source-code files (extension [‘js’])

Each subset of the Software Heritage Archive includes two files:

- A CSV file containing a row for each file of the subset, whose columns are: `swhid, file_id, length, local_path, filename, filepath`. 
  - `swhid` is the SoftWare Heritage persistent IDentifiers [SWHIDs](https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html). 
  - `file_id` is the file identifier in the local filesystem, in the Software Heritage Archive scenario it is the [SHA1](https://en.wikipedia.org/wiki/SHA-1) of the file. 
  - `length` is the length of the file in bytes. 
  - `local_path` is the path of which it file is stored locally (relative to `--input_dir`). 
  - `filename` is the original filename of the file. 
  - `filepath` is the original filename + path of the file.

- A compressed tar archive containing the actual content of the files of the subset, each one named with the SHA1 of their content.

In case you want to decompress archive you generate with `bench_PPC.py --keep` use 
```bash
./decompress_dataset.py -c <used_compressor> <first_archive> <destination_dir>
```

To decompress archive you generate with `bench_PPC.py --keep --block-size xxMiB`, so divided into blocks (file-access scenario), use, for example:
```bash
./decompress_dataset.py -c zstd /extralocal/swh/000000000_C_small_block_compressed_xxMiB.tar.zstd -o <destination_dir>
```


If you want to work on a larger dataset look at the section [Datasets](#datasets-details).


### 50GiB dataset from GitHub

There is also a 50GiB of blobs taken from the most popular repositories on GitHub (october 2022). It is made of the most popular (the one with more stars) GitHub repositories written in C/C++ and Python. The average file size is 28,89 KiB, and it contains all the versions of the source code of famous repositories like [redis](https://github.com/redis/redis), [ngnix](https://github.com/nginx/nginx), [zstd](https://github.com/facebook/zstd), [scikit-learn](https://github.com/scikit-learn/scikit-learn), [bert](https://github.com/google-research/bert), and [keras](https://github.com/keras-team/keras). The complete list of repositories included is in `dataset_generation` folder.


To download the repositories use:
```bash
./download_dataset.py -s 50GiB -o ./tmp
```

To decompress them use:
```bash
./decompress_sataset.py <full-path>/50GiB_github_filename_sort_50GiB.tar.zstd_22 --dataset -o tmp/blobs
./decompress_dataset.py <full-path>/repos_all_compressed.tar.zstd_22 --dataset -o tmp/repos --force
```

To benchmark the performance of the PPC-framework on this dataset run:

```bash
./bench_PPC.py tmp/50GiB_github.csv -i <full-path>/tmp/blobs -o /tmp -p all
```

To benchmark the perfromance of git-pack use:

```bash
./bench_git_pack.py <full-path>/tmp/repos/repos_all -o /tmp
```

## References
<a id="1">[1]</a> 
Michael Armbrust, Tathagata Das, Liwen Sun, Burak Yavuz, Shixiong Zhu, Mukul Murthy, Joseph Torres, Herman van Hovell, Adrian Ionescu, Alicja Łuszczak, Michał Świtakowski, Michał Szafrański, Xiao Li, Takuya Ueshin, Mostafa Mokhtar, Peter Boncz, Ali Ghodsi, Sameer Paranjpye, Pieter Senster, Reynold Xin, and Matei Zaharia. "Delta lake: high-performance ACID table storage over cloud object stores". Proc. VLDB Endow. 13, 12 (August 2020), 3411–3424. https://doi.org/10.14778/3415478.3415560

<a id="2">[2]</a> 
P. Ferragina and G. Manzini, "On compressing the textual web," in Proceedings of the International Conference on Web Search and Web Data Mining. 2010. https://dl.acm.org/doi/abs/10.1145/1718487.1718536 

<a id="3">[3]</a> 
Inria, “Software Heritage Archive”. Available: https://www.softwareheritage.org/

