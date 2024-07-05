#!/usr/bin/env python3

import shutil
import pandas as pd
import os
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
import getpass
from shutil import which
import sys
import subprocess
import gzip, zstandard

float_1GiB = float(2**30)
float_1MiB = float(2**20)
float_1KiB = float(2**10)


def read_file(filename):
    with open(filename, mode='rb') as file:
        all_of_it = file.read()
        return all_of_it


def stats_compress_decompress(df, dataset_name, input_dir, output_dir, compressor, keep=False):
    compressor_no_flags = os.path.basename(compressor.split()[0])
    compressed_extension = '.' + compressor_no_flags

    if compressor_no_flags == 'gzip':
        # to avoid gzip: stdin: not in gzip format
        compressed_extension = '.gz'

    total_uncompressed_size = df['length'].sum()
    total_uncompressed_size_MiB = total_uncompressed_size / float_1MiB
    total_uncompressed_size_GiB = total_uncompressed_size / float_1GiB

    str_size_GiB = str(int(round(total_uncompressed_size_GiB, 2)))

    NEW_DIR_TMP = os.path.join(
        output_dir, 'tmp.SWH_single_file_performance_dir_{}_{}_{}_{}'.format(dataset_name, str_size_GiB, compressor_no_flags, os.getpid()))
    os.makedirs(NEW_DIR_TMP)

    num_files = len(df.index)

    start_time = time.time()

    with ThreadPoolExecutor(NUM_THREAD) as executor:
        def compress_single_no_shell(file_path_comp, file_path_uncomp, compressor):
            if compressor == 'gzip':
                with gzip.open(file_path_comp, 'wb') as f_out:
                    f_out.write(read_file(file_path_uncomp))
            elif 'zstd' in compressor:
                with open(file_path_uncomp, 'rb') as f_in:
                    uncompressed_data = f_in.read()

                # because I'm using compressors like <path>/zstd_<level>_plain
                if '_' in compressor:
                    for s in compressor.split('_'):
                        if s.isnumeric():
                            level = int(s)
                    cctx = zstandard.ZstdCompressor(level=level)
                    compressed_data = cctx.compress(uncompressed_data)
                else:
                    cctx = zstandard.ZstdCompressor()
                    compressed_data = cctx.compress(uncompressed_data)

                with open(file_path_comp, 'wb') as f_out:
                    f_out.write(compressed_data)
            else:
                command = f"cat {file_path_uncomp} | {compressor} > {file_path_comp}"
                # this is very slow, is using python library for the compressor is better (gzip.open, zstd.open, etc...)
                subprocess.run(command, shell=True)

        #print(compressor)
        for i, row in df.iterrows():
            # create dir row['local_path']
            path_comp_dir = os.path.join(NEW_DIR_TMP, row['local_path'])
            os.makedirs(path_comp_dir, exist_ok=True)

            file_path_uncomp = os.path.join(
                input_dir, row['local_path'], row['file_id'])
            file_path_comp = os.path.join(
                NEW_DIR_TMP, row['local_path'], row['file_id']) + compressed_extension

            if NUM_THREAD == 1:
                compress_single_no_shell(file_path_comp, file_path_uncomp, compressor)
            else:
                executor.submit(compress_single_no_shell, file_path_comp,
                                file_path_uncomp, compressor)

    compression_time = time.time() - start_time

    # total_uncompressed_size = 0
    compressed_size = 0

    for i, row in df.iterrows():
        # file_path_uncomp = os.path.join(
        #     NEW_DIR_TMP, row['local_path'], row['file_id'])
        file_path_comp = os.path.join(
            NEW_DIR_TMP, row['local_path'], row['file_id']) + compressed_extension
        # if not os.path.exists(file_path_uncomp):
        #     print('file_path_uncomp does not exist: ' + file_path_uncomp)
        #     exit(1)

        # if not os.path.exists(file_path_comp):
        #     print('file_path_comp does not exist: ' + file_path_comp)
        #     exit(1)
        # total_uncompressed_size += os.path.getsize(file_path_uncomp)
        compressed_size += os.path.getsize(file_path_comp)
        # os.remove(file_path_uncomp)

    assert (compressed_size != 0)

    # than decompress
    # get a random subset of files
    # sample_prop = 0.1
    # df_sample = df.sample(frac=sample_prop, random_state=42)
    # num_queries = len(df_sample.index)

    def decompress_single_no_shell(file_path_comp, file_path_uncomp, compressor):
        if compressor == 'gzip':
            with gzip.open(file_path_comp, 'rb') as f_in:
                with open(file_path_uncomp, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        elif 'zstd' in compressor:
            with open(file_path_comp, 'rb') as f_in:
                compressed_data = f_in.read()

            dctx = zstandard.ZstdDecompressor()
            uncompressed_data = dctx.decompress(compressed_data)

            with open(file_path_uncomp, 'wb') as f_out:
                f_out.write(uncompressed_data)
        else:
            command = f"cat {file_path_uncomp} | {compressor} > {file_path_comp}"
            # this is very slow, is using python library for the compressor is better (gzip.open, zstd.open, etc...)
            subprocess.run(command, shell=True)
    
    # start_time = time.time()
    # with ThreadPoolExecutor(NUM_THREAD) as executor:
    #     for i, row in df_sample.iterrows():
    #         file_path_comp = os.path.join(
    #             NEW_DIR_TMP, row['local_path'], row['file_id']) + compressed_extension
    #         file_path_uncomp = os.path.join(
    #             NEW_DIR_TMP, row['local_path'], row['file_id'])
    #         # assert(os.path.exists(file_path_comp))
    #         # decompress_single(file_path_comp, file_path_uncomp)

    #         executor.submit(decompress_single,
    #                         file_path_comp, file_path_uncomp)

    # decompression_time_per_query = (time.time() - start_time) / num_queries

    # for i, row in df_sample.iterrows():
    #     file_path_uncomp = os.path.join(NEW_DIR_TMP, row['local_path'], row['file_id'])
    #     os.remove(file_path_uncomp)

    start_time = time.time()
    with ThreadPoolExecutor(NUM_THREAD) as executor:
        for i, row in df.iterrows():
            file_path_comp = os.path.join(
                NEW_DIR_TMP, row['local_path'], row['file_id']) + compressed_extension
            file_path_uncomp = os.path.join(
                NEW_DIR_TMP, row['local_path'], row['file_id'])

            if NUM_THREAD == 1:
                decompress_single_no_shell(file_path_comp, file_path_uncomp, compressor)
            else:
                executor.submit(decompress_single_no_shell,
                                file_path_comp, file_path_uncomp, compressor)

    decompression_time = time.time() - start_time
    decompression_time_per_query = decompression_time / len(df.index)

    if keep:
        with ThreadPoolExecutor(NUM_THREAD) as executor:
            def move_one_file(file_loc, new_loc):
                os.makedirs(os.path.dirname(new_loc), exist_ok=True)
                shutil.copy(file_loc, new_loc)

            for i, row in df.iterrows():
                file_loc = os.path.join(
                    NEW_DIR_TMP, row['local_path'], row['file_id']) + compressed_extension
                new_loc = os.path.join(
                    output_dir, row['local_path'], row['file_id']) + compressed_extension

                # assert(os.path.exists(file_loc))
                executor.submit(move_one_file, file_loc, new_loc)

    # with ThreadPoolExecutor(NUM_THREAD) as executor:
    #     for i, row in df.iterrows():
    #         def count_space_usage(file_path_uncomp, file_path_comp):
    #             global total_uncompressed_size
    #             total_uncompressed_size += os.path.getsize(file_path_uncomp)

    #             global compressed_size
    #             compressed_size += os.path.getsize(file_path_comp)
    #             # remove becuase I'm compressing it again to measure compression time

    #             os.remove(file_path_comp)

    #         file_path_uncomp = os.path.join(NEW_DIR_TMP, row['sha1'])
    #         file_path_comp = os.path.join(NEW_DIR_TMP, row['sha1']) + '.gz'

    #         executor.submit(count_space_usage, file_path_uncomp, file_path_comp)
    #         print('total_uncompressed_size ' + str(total_uncompressed_size), flush=True)
    #         print('compressed_size ' + str(compressed_size), flush=True)

    #         #count_space_usage(file_path_uncomp, file_path_comp)

    # delete files in new directory
    shutil.rmtree(NEW_DIR_TMP)

    avg_file_sizeKiB = df['length'].mean() / float_1KiB
    median_file_sizeKiB = df['length'].median() / float_1KiB

    throughput_files_per_second = (num_files) / float(decompression_time)

    # DATASET,NUM_FILES,TOTAL_SIZE(GiB),AVG_FILE_SIZE(KiB),MEDIAN_FILE_SIZE(KiB),TECHNIQUE,COMPRESSION_RATIO(%),ORDERING_TIME(s),COMPRESSION_TIME(s),COMPRESSION_SPEED(MiB/s),FULL_DECOMPRESSION_SPEED(MiB/s),TIME_FILE_DECOMPRESSION(ms),THROUGHPUT(files/s),NOTES
    print('{},{},{},{},{},{}+{},{},{},{},{},{},{},{},{}'.format(
        dataset_name,
        num_files,
        str(round(total_uncompressed_size_GiB, 2)),
        str(round(avg_file_sizeKiB, 2)),
        str(round(median_file_sizeKiB, 2)),
        'single_files',
        compressor_no_flags,  # compression ratio
        str(round((compressed_size / total_uncompressed_size * 100), 3)),
        '0.0',  # ordering time
        str(round(compression_time, 2)),  # compression time
        str(round((total_uncompressed_size_MiB) / \
            float(compression_time), 2)),  # compression speed
        str(round((total_uncompressed_size_MiB) / float(decompression_time), 2)), # decompression speed
        str(round(decompression_time_per_query * 1000., 2)),
        str(round(throughput_files_per_second, 2)),  # THROUGHPUT(files/s)
        f'num_threads={NUM_THREAD}'), flush=True)


# Instantiate the parser
parser = argparse.ArgumentParser(description='This script is used to measure the performance of singularly compress a set of file '
                                 '(in the list csv-file) with gzip.')

# get files lists
parser.add_argument('csv_file_list', metavar="csv-file", nargs='+',
                    help='List of files (in csv format) to compress')

parser.add_argument('-i', '--input-dir', default='/data/swh/blobs_uncompressed',
                    help='Directory where the uncompressed blobs are stored')

parser.add_argument('-o', '--output-dir', default='/extralocal/swh',
                    help='Directory used to store temporary files')

parser.add_argument('-c', '--compressor', nargs='+', default=['gzip'],
                    # (workaround here https://www.gnu.org/software/tar/manual/html_node/gzip.html)')
                    help='Compressors to apply to each file, default: gzip\n'
                         'See doc for how to pass options to a compressor')

parser.add_argument('-k', '--keep', action='store_true',
                    help='Keep the compressed files after benchmark. The resulting\n'
                    'compressed files are stored in the `--output-dir` directory',
                    default=False)

parser.add_argument('-T', '--num-thread', default=16, type=int,
                    help='Number of thread used for the compress files in parallel\n')

if __name__ == "__main__":
    args = parser.parse_args()

    print("# Start: {}. Machine: {}. User: {}.".format(time.strftime(
        '%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
        str(os.uname()[1]),
        getpass.getuser()),
        flush=True)

    print('# Taking files from : {}. Saving archives to : {} PID {}'.format(
        str(args.input_dir), str(args.output_dir), os.getpid()))

    if len(args.csv_file_list) == 0:
        parser.print_help()
        exit(1)

    NUM_THREAD = int(args.num_thread)

    print("DATASET,NUM_FILES,TOTAL_SIZE(GiB),AVG_FILE_SIZE(KiB),MEDIAN_FILE_SIZE(KiB),TECHNIQUE,COMPRESSION_RATIO(%),ORDERING_TIME(s),COMPRESSION_TIME(s),COMPRESSION_SPEED(MiB/s),FULL_DECOMPRESSION_SPEED(MiB/s),TIME_FILE_DECOMPRESSION(ms),THROUGHPUT(files/s),NOTES", flush=True)

    for dataset in args.csv_file_list:
        dataset_name = os.path.basename(dataset)

        df = pd.read_csv(dataset,
                         dtype={'swhid': 'string', 'file_id': 'string', 'length': 'Int64',
                                'filename': 'string'},
                         on_bad_lines='skip',
                         engine='python',
                         encoding_errors='ignore')

        dataset_name = dataset_name.replace('.csv', '')
        dataset_name = dataset_name.replace('_info', '')

        df.dropna(inplace=True)
        df.reset_index(inplace=True, drop=True)

        for compressor in args.compressor:
            # the compressor script must exist
            assert (os.path.isfile(compressor)
                    or which(compressor) is not None)
            stats_compress_decompress(
                df, dataset_name, args.input_dir, args.output_dir, compressor, args.keep)

    print('')
    print("# Ending time : ", time.strftime(
        '%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    sys.exit(0)
