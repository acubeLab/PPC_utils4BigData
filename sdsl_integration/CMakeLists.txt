cmake_minimum_required(VERSION 3.23)
project(csa_sdsl)

set(CMAKE_CXX_STANDARD 23)

add_subdirectory(lib/sdsl-lite)

include_directories(lib/sdsl-lite/include)
include_directories(lib/sdsl-lite/lib)
include_directories(lib/sdsl-lite/external/libdivsufsort/include)

add_executable(csa_sdsl main.cpp)
#target_include_directories(csa_sdsl INTERFACE lib/sdsl-lite/include)

target_link_libraries(csa_sdsl sdsl divsufsort divsufsort64)
