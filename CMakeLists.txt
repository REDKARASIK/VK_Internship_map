cmake_minimum_required(VERSION 3.23)

project(
    kvstorage
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 23)
include(FetchContent)
FetchContent_Declare(
  gtest
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)
FetchContent_MakeAvailable(gtest)
add_subdirectory(bin)
add_subdirectory(lib)
add_executable(kv_tests tests/kv_storage_test.cpp)
target_link_libraries(kv_tests PRIVATE kvstore GTest::gtest_main)
target_include_directories(kv_tests PUBLIC ${PROJECT_SOURCE_DIR})
enable_testing()