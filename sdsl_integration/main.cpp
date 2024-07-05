#include <iostream>
#include <sdsl/suffix_arrays.hpp>
#include <string>
#include <chrono>
#include <filesystem>
#include "mm_file.hpp"


template<class csa_t>
void csa_stats(csa_t &csa, std::string file, std::string csa_type = "") {
    size_t file_size_in_bytes = std::filesystem::file_size(file);
    size_t file_size_in_MiB = file_size_in_bytes / float(1 << 20);
    std::cout << "file size : " << file << " of size " << file_size_in_bytes << " bytes (" << file_size_in_MiB
              << " MiB)." << std::endl;
    auto start_compression = std::chrono::high_resolution_clock::now();
    sdsl::construct(csa, file, 1);

    auto stop_compression = std::chrono::high_resolution_clock::now();
    size_t size_in_bytes_csa = size_in_bytes(csa);
    size_t size_in_MiB_csa = size_in_bytes_csa / float(1 << 20);

    if (csa_type.empty())
        std::cout << sdsl::util::demangle(typeid(csa).name()) << " size : " << size_in_bytes_csa << " bytes ("
                  << size_in_MiB_csa << " MiB)." << std::endl;
    else
        std::cout << csa_type << " size : " << size_in_bytes_csa << " bytes (" << size_in_MiB_csa << " MiB)."
                  << std::endl;

    std::cout << "compression ratio : " << (float(size_in_bytes_csa) / float(file_size_in_bytes)) * 100. << "%"
              << std::endl;

    std::cout << "compression speed : "
              << float(file_size_in_MiB) /
                 (float(std::chrono::duration_cast<std::chrono::seconds>(stop_compression - start_compression).count()))
              << " MiB/s" << std::endl;

    std::vector<char> decompressed_string(file_size_in_bytes);
    auto start_decompression = std::chrono::high_resolution_clock::now();
    size_t extracted = sdsl::extract(csa, 0, file_size_in_bytes - 1, decompressed_string.begin());
    auto stop_decompression = std::chrono::high_resolution_clock::now();

    // if in debug mode
#ifndef NDEBUG
    assert(extracted == file_size_in_bytes);
    int advice = mm::advice::sequential;

    // read the stream as uint16_t integers
    mm::file_source<char> fin(file, advice);
    std::cout << "mapped " << fin.bytes() << " bytes " << std::endl;

    auto const *data = fin.data();
    for (size_t i = 0; i != fin.size(); ++i) {
        if (data[i] != 0) // 0 is a special value, we treat it in a special way
            assert(decompressed_string[i] == data[i]);
    }
    fin.close();
    std::cout << "checked correctness of the decompressed data" << std::endl;
#endif


    float decompression_speed =
            float(file_size_in_MiB) / float(std::chrono::duration_cast<std::chrono::seconds>(
                    stop_decompression - start_decompression).count());
    std::cout << "DE-compression speed : "
              << (decompression_speed)
              << " MiB/s" << std::endl;

    std::cout << "---------------" << std::endl;
}

template<int First, int Last, int step, typename Lambda>
inline void static_for(Lambda const &f) {
    if constexpr (First <= Last) {
        f(std::integral_constant<size_t, First>{});
        static_for<First + step, Last, step>(f);
    }
}

// /home/swh/50GiB_github_50GiB_big_archive
// /home/swh/50GiB_github/redis_blobs
// /home/swh/50GiB_github/brotli_blobs
int main(int argc, char *argv[]) {
    if (argc < 2) {
        return 1;
    }
    const size_t inf = (size_t(1) << 32) - 1;
    const size_t sa_sampling = inf;
    const size_t isa_sampling = 2048;

    const size_t start_exp_rrr = 6;
    const size_t end_exp_rrr = 8;

    const size_t start_exp_enc = 7;
    const size_t end_exp_enc = 9;

    for (int i = 1; i < argc; ++i) {
        std::string file = argv[i];

        static_for<start_exp_rrr, end_exp_rrr, 1>([&](auto i) {
            const size_t block_size = (1 << i) - 1;
            sdsl::csa_wt<sdsl::wt_huff<sdsl::rrr_vector<block_size>>, sa_sampling, isa_sampling> csa;
            csa_stats(csa, file, "sdsl::csa_wt<wt_huff<rrr_vector<" + std::to_string(block_size) + ">");
        });

        static_for<start_exp_enc, end_exp_enc, 1>([&](auto i) {
            const size_t block_size = (1 << i);
            sdsl::csa_sada<sdsl::enc_vector<sdsl::coder::elias_delta, block_size>, sa_sampling, isa_sampling> csa;
            csa_stats(csa, file, "sdsl::csa_sada<enc_vector<" + std::to_string(block_size) + ">>");
        });
    }
}