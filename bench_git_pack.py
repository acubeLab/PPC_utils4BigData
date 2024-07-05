#!/usr/bin/env python3

import sys
import os
import glob
import time
import subprocess
import argparse
import getpass
import pandas as pd
import zlib

Description = """
This script is used to measure the performance of the git pack command.
The git pack command is used to compress the git objects in a repository.

Preciselly all the repos in the main directory are singluarly benchmarked, which means:
they are clone in a tmp directory (--output), 
`git pack-objects` (https://www.git-scm.com/docs/git-pack-objects) is run measuring compression time. 
`git pack-objects` is the command that is used to effectivelly compress the all 
objects in the repository, starting from a specific commit, using the window and depth parameters,
without reusing the existing objects/deltas.

Then the pack files are unpacked using `git unpack-objects` (https://git-scm.com/docs/git-unpack-objects).
After it then another decompression pass is needed (to ungiz singularly the blobs).

"""

float_1GiB = float(2**30)
float_1MiB = float(2**20)
float_1KiB = float(2**10)

REPO_DIR = os.path.dirname(os.path.realpath(__file__))


def exec_cmd(cmd, to_print=False):
    if to_print:
        print('Executing -> ' + cmd)
        print('Executing -> ' + str(cmd.split()))
    process = subprocess.Popen(
        cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    output, error = process.communicate()
    if error != '':
        return output
    else:
        print('ERROR --> ' + error)
        return output


def exec_cmd_checkoutput(cmd, to_print=False):
    if to_print:
        print('Executing -> ' + cmd)
        print('Executing -> ' + str(cmd.split()))
    output = subprocess.check_output(cmd.split())
    return output


def read_file(filename):
    with open(filename, mode='r', encoding="utf-8", errors="ignore") as file:
        all_of_it = file.read()
        return all_of_it


def read_file_bytes(filename):
    with open(filename, mode='rb') as file:
        all_of_it = file.read()
        return all_of_it

# clone repo into current directory and return the time to compress it by the server
def exec_git_clone_command_time(repo_path, is_bare):
    # print('Starting with repo: ' + repo_path)
    if is_bare:
        p1cmd = 'git clone --progress --bare  file://' + repo_path
    else:
        p1cmd = 'git clone --progress file://' + repo_path
    p1 = subprocess.Popen(
        p1cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    remote_flag = False
    # Counting how much time for compression
    start = 0
    end = 0
    # git clone writes on stderr
    for line in iter(p1.stderr.readline, ""):
        # print(line, end='')
        if not remote_flag and b'Compressing objects' in line:
            # print('Start counting')
            remote_flag = True
            start = time.time()
        elif remote_flag and b'Compressing objects' not in line:
            # print('End counting')
            remote_flag = False
            end = time.time()
            # break
        elif b'Resolving deltas: 100%' in line:
            p1.wait()
            break
    compression_time = (end - start)
    assert (compression_time > 0)
    return compression_time

# clone repo into current directory


def exec_git_clone_command(repo_path, is_bare):
    if is_bare:
        p1cmd = 'git clone --bare  file://' + repo_path
    else:
        p1cmd = 'git clone file://' + repo_path
    
    try:
        p1 = subprocess.Popen(
            p1cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # output, error = p1.communicate()
        # if error != b'' and error != None:
        #     print(f"// ! Error while cloning the repo: {repo_path}")
        #     print(error)
        #     return False
        
        p1.wait()
        return True

    except Exception as e:
        print(f"// Exception while cloning the repo: {repo_path}")
        print(f"// Error: {e}")
        return False


def blob_size(blob):
    # return len(subprocess.check_output(['git', 'cat-file', 'blob', blob[0]]))
    return int(subprocess.check_output(['git', 'cat-file', '-s', blob[0]]))


def blob_save(blob, blobs_target_dir):
    out = subprocess.check_output(['git', 'cat-file', 'blob', blob[0]])
    with open(os.path.join(blobs_target_dir, blob[0]), 'wb') as w:
        w.write(out)
    return len(out)


def blob_save_2levels(blob, blobs_target_dir):
    out = subprocess.check_output(['git', 'cat-file', 'blob', blob[0]])
    with open(os.path.join(blobs_target_dir, blob[0][:2], blob[0]), 'wb') as w:
        w.write(out)
    return len(out)


def get_all_blobs_from_pack(pack_file):
    # use git verify-pack -v
    # the first one is the first and only commit, the last two are 'ok' and []
    blobs_in_pack_file = [(s.split()) for s in
                          exec_cmd_checkoutput(
                              "git verify-pack -v " + pack_file)
                          .decode('utf-8', 'ignore')
                          .split('\n')[1:-2]]

    # consider only the blobs, git verify-pack also return stats about chains length etc...
    blobs_in_pack_file = [b for b in blobs_in_pack_file if b[1] == 'blob']
    return blobs_in_pack_file


def get_all_blobs(commit='--all'):
    # -1 due to the last `\n`
    blobs = [(s[:40], s[41:])
             for s in exec_cmd_checkoutput(
        f"git rev-list --objects --filter=object:type=blob {commit}")
        .decode('utf-8', 'ignore')
        .split('\n')[1:-1]]
    return blobs


def single_repo_compress_decompress(repo_path, depth, window, commit):
    repo_name = os.path.basename(repo_path)

    repo_path_workspace = os.path.join(
        args.output, f"tmp.git_pack_performance_{os.getpid()}_{depth}_{window}", repo_name)

    # exec_cmd('rm -rf ' + repo_path_workspace)
    os.makedirs(repo_path_workspace)
    # move to repo_path_workspace
    os.chdir(repo_path_workspace)

    if not exec_git_clone_command(repo_path, args.bare):
        print(f"// Error while cloning the repo: {repo_path}")
        sys.exit(1)

    if args.bare:
        new_repo_path = os.path.join(repo_path_workspace, repo_name + '.git')
    else:
        new_repo_path = os.path.join(repo_path_workspace, repo_name)

    assert(os.path.exists(new_repo_path))
    os.chdir(new_repo_path)

    blobs = get_all_blobs(commit)

    assert (len(blobs) > 0)

    results = map(blob_size, blobs)
    full_size = sum(results)
    num_blobs = len(blobs)

    start_time = time.time()

    if sys.version_info < (3, 9):
        command_to_exec = f"git pack-objects pack --filter=object:type=blob --revs --window={str(window)} --depth={str(depth)} --no-reuse-delta --no-reuse-object --threads={args.num_threads}"
        process = subprocess.Popen(command_to_exec.split(
        ), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        commit_to_start = (commit + '\n').encode()
        process.communicate(input=commit_to_start)
        process.wait()

        # get output and error
        output, error = process.communicate()
        if error != b'' and error != None:
            print('! Error while packing')
            print(error)
            sys.exit(1)

        new_pack_files = glob.glob('pack-*')

        assert (len(new_pack_files) == 3)
    else:
        # when using python3.8 you have different git version with respect python3.11
        # the git behavior is different, so we need to use a slightly different command
        # the errorr is: fatal: cannot use --filter without --stdout\n
        command_to_exec = f"git pack-objects --stdout --filter=object:type=blob --revs --window={str(window)} --depth={str(depth)} --no-reuse-delta --no-reuse-object --threads={args.num_threads}"
        # redirect stdout to pack file
        process = subprocess.Popen(command_to_exec.split(
        ), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        commit_to_start = (commit + '\n').encode()
        output, error = process.communicate(input=commit_to_start)
        #print(f"// Output: {output}")
        # store the output in a file
        with open('pack-new_pack_file.pack', 'wb') as f:
            f.write(output)
        
        new_pack_files = ['pack-new_pack_file.pack']


    if args.bare:
        pack_path = os.path.join(new_repo_path, 'objects/pack/')
    else:
        pack_path = os.path.join(new_repo_path, '.git/objects/pack/')

    old_pack_files = glob.glob(os.path.join(pack_path, 'pack*.pack'))

    for old_pack_file in old_pack_files:
        os.remove(old_pack_file)

    for new_pack_file in new_pack_files:
        os.rename(new_pack_file, os.path.join(pack_path, new_pack_file))

    pack_file_path = os.path.join(pack_path, new_pack_files[0])
    pack_file = new_pack_files[0]

    assert (os.path.exists(pack_file_path))

    pack_time = time.time() - start_time

    pack_files_size = os.path.getsize(pack_file_path)

    start_time = time.time()

    exec_cmd('mv ' + pack_file_path + ' .')
    pack_file_path = os.path.join(new_repo_path, pack_file)

    # exec git unpack-objects
    with open(pack_file_path, 'r') as infile:
        exec_cmd('rm -rf ' + pack_file[:-5] + '.idx')
        process = subprocess.Popen(
            'git unpack-objects -q'.split(), stdin=infile, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()
        output, error = process.communicate()
        if args.verbose and output != b'' and output != None:
            print(output)
        if error != b'' and error != None:
            print('! Error while unpacking ' + pack_file_path)
            print(error)

    start_second_pass_time = time.time()

    blobs_target_dir = os.path.join(
        new_repo_path, 'uncompressed_objects')
    # create directory for uncompressed blobs

    chars = ['0', '1', '2', '3', '4', '5', '6',
             '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']

    directories = [c1+c2 for c1 in chars for c2 in chars]

    os.makedirs(blobs_target_dir)

    for d in directories:
        os.makedirs(os.path.join(blobs_target_dir, d))

    CAT_FILE_DECOMPRESS = False

    if CAT_FILE_DECOMPRESS:
        # exec git cat-file
        for blob in blobs:
            blob_save_2levels(blob, blobs_target_dir)

    else:
        all_blobs_location = ''
        if args.bare:
            all_blobs_location = 'objects/**/*'

        else:
            all_blobs_location = '.git/objects/**/*'

        # ungzip all files
        for compressed_blob in glob.glob(all_blobs_location):
            if 'pack' in compressed_blob:
                continue
            blob_id = os.path.basename(compressed_blob)
            uncompressed_blob = os.path.join(
                'uncompressed_objects', blob_id[:2], blob_id)
            with open(uncompressed_blob, 'wb') as f_out:
                all_file_content = read_file_bytes(compressed_blob)
                f_out.write(zlib.decompress(all_file_content))

    decompression_time = time.time() - start_time
    second_time = time.time() - start_second_pass_time

    if CAT_FILE_DECOMPRESS:
        if args.verbose:
            print(f"// CAT_FILE_DECOMPRESS time, {second_time:.2f}, seconds")
    else:
        if args.verbose:
            print(f"// Gzip time, {second_time:.2f}, seconds")

    os.chdir(REPO_DIR)

    exec_cmd('rm -rf ' + repo_path_workspace)

    return [num_blobs, full_size, pack_files_size, pack_time, decompression_time]


def git_pack_compress_decompress_performace(dataset_name, repos_path, window, depth):

    list_repos = [x for x in os.listdir(
        repos_path) if os.path.isdir(os.path.join(repos_path, x))]

    assert (len(list_repos) > 0)

    if args.verbose:
        print(
            f"// Compress decompress single repo verbose output: {dataset_name}")
        print("// repo_name,num_blobs,full_size(MiB),method,compression_ratio(%),compressed_size(MiB),compression_speed(MiB/s),decompression_speed(MiB/s),compression_time(s),decompression_time(s),window,depth", flush=True)

    full_size = 0
    compressed_size = 0
    compression_time = 0
    decompression_time = 0
    num_files = 0

    CSV_DIR = os.path.join(REPO_DIR, 'dataset_generation')
    df_details = pd.concat([pd.read_csv(os.path.join(CSV_DIR, f)) for f in [
                           'repos-C-25GiB.csv', 'repos-Python-25GiB.csv']], ignore_index=True)

    for repo_path in list_repos:
        commit = df_details[df_details['repo_name']
                            == repo_path]['last_commit'].values[0]

        results = single_repo_compress_decompress(os.path.join(
            repos_path, repo_path), depth, window, commit)
        this_num_files = results[0]
        this_full_size = results[1]
        this_compressed_size = results[2]
        this_compression_time = results[3]
        this_decompression_time = results[4]

        if args.verbose:
            repo_name = os.path.basename(repo_path)
            this_compress_ratio = this_compressed_size / this_full_size * 100

            this_compression_speed = (this_full_size / float_1MiB) / \
                float(this_compression_time)

            this_decompression_speed = (this_full_size / float_1MiB) /  \
                float(this_decompression_time)

            print(
                f"// {repo_name},{this_num_files},{this_full_size / float_1MiB:.2f},single_repo_compress_decompress,"
                f"{this_compress_ratio:.2f},{this_compressed_size / float_1MiB:.2f},"
                f"{this_compression_speed:.2f},{this_decompression_speed:.2f},{this_compression_time:.2f},{this_decompression_time:.2f},{window},{depth}", flush=True)

        full_size += this_full_size
        num_files += this_num_files
        compressed_size += this_compressed_size
        compression_time += this_compression_time
        decompression_time += this_decompression_time

    assert (full_size != 0)
    assert (num_files != 0)
    assert (compressed_size != 0)
    assert (compression_time != 0)
    assert (decompression_time != 0)

    # DATASET,NUM_FILES,TOTAL_SIZE(GiB),TECHNIQUE,COMPRESSION_RATIO(%),COMPRESSION_SPEED(MiB/s),DECOMPRESSION_SPEED(MiB/s),COMPRESSION_TIME(s),DECOMPRESSION_TIME(s),NOTES

    print(f"{dataset_name},{num_files},{full_size / float_1GiB:.2f},git-pack(singularly),"
          f"{compressed_size / full_size * 100:.2f},"
          f"{(full_size / float_1MiB) / float(compression_time):.2f},"
          f"{(full_size / float_1MiB) / float(decompression_time):.2f},"
          f"{compressed_size / float_1GiB:.2f},{compression_time:.2f},{decompression_time:.2f},window={window}_depth={depth}", flush=True)


# https://www.git-scm.com/docs/git-repack
default_window = 10

default_depth = 50

# Instantiate the parser: options and arguments are in the global variable args
parser = argparse.ArgumentParser(
    description=Description, formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('main', metavar="main-directory", type=str,
                    help='directory in which the repos are')
parser.add_argument('-o', '--output', type=str, default='/extralocal/swh',
                    help='output directory, where the temporary files are saved')
parser.add_argument('-v', '--verbose', action='store_true',
                    help='verbose output')
parser.add_argument('-b', '--bare', action='store_true', default=False,
                    help='changes if the repos are cloned with --bare or not.')
parser.add_argument('-T', '--num-threads', type=int, default=1,
                    help='number of threads')
parser.add_argument('-W', '--window', nargs='+', default=[default_window],
                    help='list of values for `window` to test')
parser.add_argument('-D', '--depth', nargs='+', default=[default_depth],
                    help='list of values for `depth` to test')

args = parser.parse_args()

if __name__ == '__main__':
    print('# Start: {}. Machine: {}. User: {}. Saving archives to {}. PID {}.'
          .format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                  str(os.uname()[1]),
                  getpass.getuser(),
                  str(args.output),
                  os.getpid()),
          flush=True)

    assert (len(args.window) == len(args.depth))

    def is_bare_str():
        if args.bare:
            return 'bare'
        else:
            return 'not_bare'

    repos_dir = args.main

    print('DATASET,NUM_FILES,TOTAL_SIZE(GiB),TECHNIQUE,COMPRESSION_RATIO(%),'
          'COMPRESSION_SPEED(MiB/s),DECOMPRESSION_SPEED(MiB/s),COMPRESSED_SIZE(GiB),COMPRESSION_TIME(s),DECOMPRESSION_TIME(s),NOTES', flush=True)

    for (window, depth) in zip(args.window, args.depth):
        git_pack_compress_decompress_performace(
            f"git_pack_compress_decompress_performace({is_bare_str()})", repos_dir, window, depth)

    print('')
    print("# Ending time : ", time.strftime(
        '%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    sys.exit(0)
