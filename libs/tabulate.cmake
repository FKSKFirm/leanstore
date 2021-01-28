include(ExternalProject)
find_package(Git REQUIRED)

ExternalProject_Add(
        tabulate_src
        PREFIX "vendor/tabulate"
        GIT_REPOSITORY "https://github.com/p-ranav/tabulate.git"
        GIT_TAG 718d827cf05c2e9bba17e926cac2d7ab2356621c
        TIMEOUT 10
        BUILD_COMMAND ""
        UPDATE_COMMAND "" # to prevent rebuilding everytime
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/tabulate
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
)

ExternalProject_Get_Property(tabulate_src source_dir)
set(TABULATE_INCLUDE_DIR ${source_dir}/include)
file(MAKE_DIRECTORY ${TABULATE_INCLUDE_DIR})
add_library(tabulate INTERFACE IMPORTED)
set_property(TARGET tabulate APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${TABULATE_INCLUDE_DIR})
add_dependencies(tabulate tabulate_src)
