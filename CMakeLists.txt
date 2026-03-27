cmake_minimum_required(VERSION 3.16)
project(HyperDB LANGUAGES CXX C)

# ---------------------------------------------------------------------------
# C++ standard
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# force multicore build on msvc because life is too short to wait for cl.exe
# and statically link the runtime because dll hell is real.
if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

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

add_library(HyperDB STATIC ${HYPERDB_SOURCES})

# ensure -fPIC on linux so we can link this into a shared python module.
# without this, it's just a 0kb paperweight on linux.
set_target_properties(HyperDB PROPERTIES POSITION_INDEPENDENT_CODE ON)

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
if(MSVC)
    # msvc is a picky child and doesn't like -Wall -Wextra. 
    # why did i choose c++ again? anyway, here's some msvc spice.
    target_compile_options(HyperDB PRIVATE 
        /W3 /WX- /MP
        $<$<CONFIG:Release>:/O2 /Oi /GL>
        $<$<CONFIG:Debug>:/Od /Zi /RTC1>
    )
    # i hope this works. if it doesn't, it's probably microsoft's fault.
else()
    # gcc or clang, at least they understand simple flags. usually.
    target_compile_options(HyperDB PRIVATE
        -Wall -Wextra -Wno-unused-parameter
        $<$<CONFIG:Release>:-O2>
        $<$<CONFIG:Debug>:-O0 -g>
    )
    # if you're reading this on linux, i envy you.
endif()

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
