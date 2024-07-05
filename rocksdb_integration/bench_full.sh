#!/bin/bash

# TODO: change with actual location
source /home/boffa/git-pack_on_SoftwareHeritage/ppc_venv/bin/activate

ROCKSDB_EXE=/home/boffa/rocksdb/cmake-build-release-optane/examples/compressed_archive_index

#DATASET can be Debug or Python or Java or Javascript or C
DATASET=$1

if [ "$DATASET" = "Debug" ]; then
    DEBUG=true
else
    DEBUG=false
    SIZE=$2
fi

if [ "$DEBUG" = true ]; then
    FILENAME=/home/boffa/git-pack_on_SoftwareHeritage/examples/C_small.csv
else
    FILENAME=/disk2/data/${SIZE}/${DATASET}_selection/${DATASET}_selection_info.csv
fi

if [ ! -f "$FILENAME" ]; then
    echo "$FILENAME does not exist!"
    exit 1
fi

COMPRESSOR=/home/boffa/git-pack_on_SoftwareHeritage/compressor_flags/zstd_12_T16

COMPRESSOR_FA=/home/boffa/git-pack_on_SoftwareHeritage/compressor_flags/zstd_12

PERMUTERS=("random" "filename" "filename-path" "simhashsort" "tlshsort" "ssdeepsort" "minhashgraph")
PERMUTER_NAMES=("random_order" "filename_sort" "filename+path_sort" "simhash_sort" "TLSH_sort" "ssdeep_sort" "minhash_graph_tlshsort_uf_f256_r64")

BLOCK_SIZES=("256KiB" "2MiB")

for ((i=0;i<${#BLOCK_SIZES[@]};i++)) {
     ALL_BLOCK_SIZES+="${BLOCK_SIZES[$i]} "
}
#echo "${ALL_BLOCK_SIZES%,*}"

for ((i=0;i<${#PERMUTERS[@]};i++)) {
     ALL_PERMUTERS+="${PERMUTERS[$i]} "
}
#echo "${ALL_PERMUTERS%,*}"


pid=$$
COMPRESSOR_BASENAME=$(basename $COMPRESSOR)

INPUT_DIR=/data/swh/blobs_uncompressed
OUTPUT_DIR=/extralocal/swh

# Debug
if [ "$DEBUG" = true ]; then
    WORKING_DIR_PATH=${OUTPUT_DIR}/BENCH_ALL_C_small_${COMPRESSOR_BASENAME}_${pid}
else
    WORKING_DIR_PATH=${OUTPUT_DIR}/BENCH_ALL_${DATASET}_${SIZE}_${COMPRESSOR_BASENAME}_${pid}
fi

mkdir -p $WORKING_DIR_PATH

/home/boffa/git-pack_on_SoftwareHeritage/bench_PPC_full.py $FILENAME -c $COMPRESSOR -p $ALL_PERMUTERS -o $WORKING_DIR_PATH -i $INPUT_DIR

/home/boffa/git-pack_on_SoftwareHeritage/bench_single_blob.py $FILENAME -c $COMPRESSOR_FA -o $WORKING_DIR_PATH -i $INPUT_DIR
#SWH baseline
#/home/boffa/git-pack_on_SoftwareHeritage/bench_single_blob.py $FILENAME -c gzip -o $WORKING_DIR_PATH

rm -rf $WORKING_DIR_PATH

for i in "${!PERMUTERS[@]}"; do

    COMPRESSOR_BASENAME_FA=$(basename $COMPRESSOR_FA)

    if [ "$DEBUG" = true ]; then
        WORKING_DIR_PATH=${OUTPUT_DIR}/BLOCK_COMPRESSED_ROCKSDB_C_small_${PERMUTER_NAMES[$i]}_${COMPRESSOR_BASENAME_FA}_${pid}
    else
        WORKING_DIR_PATH=${OUTPUT_DIR}/BENCH_ALL_BLOCK_ROCKSDB_${DATASET}_${PERMUTER_NAMES[$i]}_${SIZE}_${COMPRESSOR_BASENAME_FA}_${pid}
    fi

    mkdir -p $WORKING_DIR_PATH

    # create the tar archives
    # TODO: check if there is already a compressed version of the archive
    # in that case avoid generating it
    # to do so the directory WORKING_DIR_PATH should be named with FILENAME COMRESSOR SIZE BLOCKSIZE E PERMUTER
    /home/boffa/git-pack_on_SoftwareHeritage/bench_PPC.py $FILENAME -c $COMPRESSOR_FA -p "${PERMUTERS[$i]}" -b $ALL_BLOCK_SIZES -k -o $WORKING_DIR_PATH -i $INPUT_DIR

    for BLOCK_SIZE in "${BLOCK_SIZES[@]}"; do
        #echo $BLOCK_SIZE

        if [ "$DEBUG" = true ]; then
            ARCHIVE_MAP=filename_archive_map_C_small_${PERMUTER_NAMES[$i]}_0GiB_${BLOCK_SIZE}.txt
        else
            ARCHIVE_MAP=filename_archive_map_${DATASET}_selection_${PERMUTER_NAMES[$i]}_${SIZE}_${BLOCK_SIZE}.txt
        fi

        if [ ! -f "$WORKING_DIR_PATH/$ARCHIVE_MAP" ]; then
            echo "$WORKING_DIR_PATH/$ARCHIVE_MAP does not exist!"
            exit 1
        fi

        $ROCKSDB_EXE $WORKING_DIR_PATH/"$ARCHIVE_MAP" $COMPRESSOR_FA $WORKING_DIR_PATH $WORKING_DIR_PATH B

        rm $WORKING_DIR_PATH/"$ARCHIVE_MAP"

        if [ "$DEBUG" = true ]; then
            find $WORKING_DIR_PATH/ -maxdepth 1 -name "*_C_small_${PERMUTER_NAMES[$i]}_0GiB_block_compressed_${BLOCK_SIZE}.tar.${COMPRESSOR_BASENAME_FA}" -exec rm -rf {} \;
        else
            find $WORKING_DIR_PATH/ -maxdepth 1 -name "*_${DATASET}_selection_${PERMUTER_NAMES[$i]}_${SIZE}_block_compressed_${BLOCK_SIZE}.tar.${COMPRESSOR_BASENAME_FA}" -exec rm -rf {} \;
        fi

        echo "$FILENAME ${COMPRESSOR_BASENAME_FA} ${PERMUTERS[$i]} $BLOCK_SIZE --> Done!"

    done
    rm -rf $WORKING_DIR_PATH
done


deactivate