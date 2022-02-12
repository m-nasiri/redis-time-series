cmake_minimum_required(VERSION 3.16)

project(redis_time_series_test
    VERSION 1.0
    DESCRIPTION "scada project test"
    LANGUAGES C CXX)

find_package(hiredis REQUIRED)
find_package(redis++ REQUIRED)
find_package(fmt REQUIRED)
find_package(GTest REQUIRED)

add_executable(${PROJECT_NAME} main.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/include
    ${hiredis_INCLUDE_DIRS} 
    ${redis++_INCLUDE_DIRS}
    ${fmt_INCLUDE_DIRS} 
    ${GTest_INCLUDE_DIRS} )

target_link_libraries(${PROJECT_NAME} PRIVATE 
    ${hiredis_LIBRARIES}
    ${redis++_LIBRARIES} 
    ${fmt_LIBRARIES} 
    ${GTest_LIBRARIES})  

add_test(NAME redis_time_series_test
    COMMAND redis_time_series_test)
