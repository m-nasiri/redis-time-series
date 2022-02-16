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

set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
string(APPEND CMAKE_SHARED_LINKER_FLAGS "-static-libgcc -static-libstdc++")

add_definitions(/DSPDLOG_FMT_EXTERNAL)

set(default_build_type "Debug")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
        STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()


include(${CMAKE_BINARY_DIR}/conan_paths.cmake)
set(CONAN_CMAKE_SILENT_OUTPUT TRUE)

add_subdirectory(include)

enable_testing()
add_subdirectory(test)
