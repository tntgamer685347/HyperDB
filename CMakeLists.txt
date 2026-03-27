cmake_minimum_required(VERSION 3.16)
project(HyperDB LANGUAGES CXX C)

# ---------------------------------------------------------------------------
# C++ standard
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------
# Source files  (paths are relative to this CMakeLists.txt, mirroring the
# vcxproj which lives one directory deeper at HyperDB/HyperDB.vcxproj)
# ---------------------------------------------------------------------------
set(HYPERDB_SOURCES
    HyperDB/HyperDB.cpp
    HyperDB/include/bcrypt/bcrypt.cpp
    HyperDB/include/bcrypt/blowfish.cpp
    HyperDB/include/tinyAES/aes.c          # compiled as C
)

# ---------------------------------------------------------------------------
# Static library target
# ---------------------------------------------------------------------------
add_library(HyperDB STATIC ${HYPERDB_SOURCES})

# ---------------------------------------------------------------------------
# Include directories
# ---------------------------------------------------------------------------
target_include_directories(HyperDB PUBLIC
    HyperDB                   # for HyperDB.h
    HyperDB/include           # flatbuffers, json.hpp, pybind11, tinyAES,
                              # bcrypt, pbkdf2, hyperdb_generated.h
)

# ---------------------------------------------------------------------------
# Python 3 (replaces the bundled python3.lib / python311.lib from Windows)
# ---------------------------------------------------------------------------
find_package(Python3 REQUIRED COMPONENTS Development)
target_include_directories(HyperDB PUBLIC ${Python3_INCLUDE_DIRS})
target_link_libraries(HyperDB PUBLIC ${Python3_LIBRARIES})

# ---------------------------------------------------------------------------
# Compiler definitions  (mirror the vcxproj per-config defines)
# ---------------------------------------------------------------------------
target_compile_definitions(HyperDB PRIVATE
    $<$<CONFIG:Debug>:_DEBUG>
    $<$<CONFIG:Release>:NDEBUG>
)

# ---------------------------------------------------------------------------
# Compiler flags
# ---------------------------------------------------------------------------
target_compile_options(HyperDB PRIVATE
    # Warnings (roughly equivalent to /W3)
    -Wall -Wextra -Wno-unused-parameter

    # Release: optimise (mirrors /O2 + /Oi + /GL)
    $<$<CONFIG:Release>:-O2>

    # Debug: no optimisation, keep debug info
    $<$<CONFIG:Debug>:-O0 -g>
)

# ---------------------------------------------------------------------------
# Linker flags
# ---------------------------------------------------------------------------
# flatbuffers is header-only in this project (no separate .so needed).
# pybind11  is header-only in this project.
# tinyAES   is compiled directly from source above.
# bcrypt    is compiled directly from source above.
# If you later need the system flatbuffers shared lib, uncomment:
#   find_package(Flatbuffers REQUIRED)
#   target_link_libraries(HyperDB PUBLIC flatbuffers::flatbuffers)

# ---------------------------------------------------------------------------
# Installation (optional – remove if not needed)
# ---------------------------------------------------------------------------
install(TARGETS HyperDB ARCHIVE DESTINATION lib)
install(FILES HyperDB/HyperDB.h DESTINATION include)
