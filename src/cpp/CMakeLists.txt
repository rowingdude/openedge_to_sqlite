cmake_minimum_required(VERSION 3.10)
project(oe2sqlite VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set build type if not specified
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Set compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# Find required packages
find_package(ODBC REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)

include_directories(/usr/include/nlohmann)

set(SOURCES
    main.cpp
    Config.cpp
    Logger.cpp
    SyncState.cpp
    DataSyncManager.cpp
    HashStorage.cpp
    HashCalculator.cpp
    DatabaseConnector.cpp
    SqliteHelper.cpp
    OdbcHelper.cpp
    TableSyncer.cpp
)

set(HEADERS
    Config.h
    Logger.h
    SyncState.h
    DataSyncManager.h
    HashStorage.h
    HashCalculator.h
    DatabaseConnector.h
    SqliteHelper.h
    OdbcHelper.h
    TableSyncer.h
    TableInfo.h
)

add_executable(${PROJECT_NAME} ${SOURCES} ${HEADERS})

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${ODBC_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    ${SQLITE3_INCLUDE_DIRS}
)

target_link_libraries(${PROJECT_NAME} PRIVATE
    ${ODBC_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARIES}
    ${OPENSSL_SSL_LIBRARIES}
    ${SQLITE3_LIBRARIES}
    sqlite3
)

install(TARGETS ${PROJECT_NAME} DESTINATION bin)

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "C++ flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "SQLite3 libraries: ${SQLITE3_LIBRARIES}")
message(STATUS "ODBC libraries: ${ODBC_LIBRARIES}")
message(STATUS "OpenSSL libraries: ${OPENSSL_LIBRARIES}")