cmake_minimum_required(VERSION 3.16)
project(HyperDB-Test LANGUAGES CXX)

# ---------------------------------------------------------------------------
# C++ standard
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------
# Executable
# ---------------------------------------------------------------------------
add_executable(HyperDB-Test
    HyperDB-Test/main.cpp
)

# ---------------------------------------------------------------------------
# Include directories
# ---------------------------------------------------------------------------
target_include_directories(HyperDB-Test PRIVATE
    HyperDB-Test/include          # HyperDB.h, hyperdb_generated.h, flatbuffers/
)

# ---------------------------------------------------------------------------
# Link against the pre-built HyperDB static library
# On Linux  → libHyperDB.a  (placed at HyperDB-Test/include/lib/libHyperDB.a)
# On Windows → HyperDB.lib  (placed at HyperDB-Test/include/lib/HyperDB.lib)
# ---------------------------------------------------------------------------
target_link_libraries(HyperDB-Test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/HyperDB-Test/include/lib/libHyperDB.a
)

# ---------------------------------------------------------------------------
# Python 3 – required because libHyperDB.a was built with pybind11
# ---------------------------------------------------------------------------
find_package(Python3 REQUIRED COMPONENTS Development)
target_include_directories(HyperDB-Test PRIVATE ${Python3_INCLUDE_DIRS})
target_link_libraries(HyperDB-Test PRIVATE ${Python3_LIBRARIES})

# ---------------------------------------------------------------------------
# Compiler definitions
# ---------------------------------------------------------------------------
target_compile_definitions(HyperDB-Test PRIVATE
    $<$<CONFIG:Debug>:_DEBUG _CONSOLE>
    $<$<CONFIG:Release>:NDEBUG _CONSOLE>
)

# ---------------------------------------------------------------------------
# Compiler flags
# ---------------------------------------------------------------------------
target_compile_options(HyperDB-Test PRIVATE
    -Wall -Wextra -Wno-unused-parameter
    $<$<CONFIG:Release>:-O2>
    $<$<CONFIG:Debug>:-O0 -g>
)
