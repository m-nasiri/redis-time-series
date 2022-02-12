cmake_minimum_required(VERSION 3.16)

project(redis_time_series
    VERSION 1.0
    DESCRIPTION "scada project"
    LANGUAGES C CXX)


find_package(hiredis REQUIRED)
find_package(redis++ REQUIRED)
find_package(fmt REQUIRED)

add_library(${PROJECT_NAME} redis_time_series.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${hiredis_INCLUDE_DIRS} 
    ${redis++_INCLUDE_DIRS}
    ${fmt_INCLUDE_DIRS} )

target_link_libraries(${PROJECT_NAME} PRIVATE 
    ${hiredis_LIBRARIES}
    ${redis++_LIBRARIES} 
    ${fmt_LIBRARIES} )  