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

set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
string(APPEND CMAKE_SHARED_LINKER_FLAGS "-static-libgcc -static-libstdc++")

include(${CMAKE_BINARY_DIR}/conan_paths.cmake)
set(CONAN_CMAKE_SILENT_OUTPUT TRUE)
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ON)

find_package(hiredis REQUIRED)
find_package(redis++ REQUIRED)
find_package(fmt REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/include
    ${hiredis_INCLUDE_DIRS} 
    ${redis++_INCLUDE_DIRS}
    ${fmt_INCLUDE_DIRS}
    )

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(${PROJECT_NAME} ${hiredis_LIBRARIES} ${redis++_LIBRARIES} ${fmt_LIBRARIES})  