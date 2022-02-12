cmake_minimum_required(VERSION 3.16)

project(redis_time_series
    VERSION 1.0
    DESCRIPTION "scada project"
    LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC") 
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

add_definitions(/DSPDLOG_FMT_EXTERNAL)

include(${CMAKE_BINARY_DIR}/conan_paths.cmake)
set(CONAN_CMAKE_SILENT_OUTPUT TRUE)

add_subdirectory(include)

enable_testing()
add_subdirectory(test)
