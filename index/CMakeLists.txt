cmake_minimum_required(VERSION 3.20)

project(
    index
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(GLOB INDEX_SRC CONFIGURE_DEPENDS "*.cpp")

# Remove the two mains so they’re not double-compiled
list(REMOVE_ITEM COMMON_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/test.cpp"
)

# Main executable
add_executable(index
    main.cpp
    ${COMMON_SRC}
)
target_include_directories(index PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Test executable
add_executable(tests
    test.cpp
    ${COMMON_SRC}
)
target_include_directories(tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})