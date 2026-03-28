cmake_minimum_required(VERSION 3.16)
project(HyperDB-Test LANGUAGES CXX)

# ---------------------------------------------------------------------------
# C++ standard
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# statically link the msvc runtime so you can run it on a fresh vm
# without downloading a 50mb redistributable because life is pain.
if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

add_executable(HyperDB-Test
    HyperDB-Test/main.cpp
)

# stay consistent with the rest of the dumpster fire.
set_target_properties(HyperDB-Test PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ---------------------------------------------------------------------------
# Include directories
# ---------------------------------------------------------------------------
target_include_directories(HyperDB-Test PRIVATE
    HyperDB-Test/include          # HyperDB.h, hyperdb_generated.h, flatbuffers/
)

# ---------------------------------------------------------------------------
# Link against the pre-built HyperDB static library
# i'm choosing to link against the .a or .lib manually. don't ask why.
# ---------------------------------------------------------------------------
if(MSVC)
    target_link_libraries(HyperDB-Test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/HyperDB-Test/include/lib/HyperDB.lib
    )
else()
    target_link_libraries(HyperDB-Test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/HyperDB-Test/include/lib/libHyperDB.a
    )
endif()

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
if(MSVC)
    # i've spent too much time here already. just work already.
    target_compile_options(HyperDB-Test PRIVATE 
        /W3 /WX- /MP
        $<$<CONFIG:Release>:/O2>
        $<$<CONFIG:Debug>:/Od /Zi>
    )
else()
    # gcc flags for people with better things to do.
    target_compile_options(HyperDB-Test PRIVATE
        -Wall -Wextra -Wno-unused-parameter
        $<$<CONFIG:Release>:-O2>
        $<$<CONFIG:Debug>:-O0 -g>
    )
endif()
