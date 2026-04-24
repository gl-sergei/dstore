cmake_minimum_required(VERSION 3.16)

project(dstore VERSION 0.1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_SKIP_RPATH TRUE)

if (DEFINED UTILS_PATH)
    set(UTILS_HOME ${UTILS_PATH} CACHE PATH "Utils output path from external input" FORCE)
    message(STATUS "UTILS_PATH is set, UTILS_HOME overridden to: ${UTILS_HOME}")
endif()

# Set library and binary output directory
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if (DEFINED LOCAL_LIB_PATH)
    set(ENV{LOCAL_LIB_PATH} ${LOCAL_LIB_PATH})
endif()
set(LOCAL_LIB_PATH $ENV{LOCAL_LIB_PATH})
message(STATUS "LOCAL_LIB_PATH : ${LOCAL_LIB_PATH}")
message(STATUS "install prefix : ${CMAKE_INSTALL_PREFIX}")

# dstore output path
set(DSTORE_COMMON_DEFAULT_OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/output)
if((DEFINED CMAKE_INSTALL_PREFIX))
    if(${CMAKE_INSTALL_PREFIX} STREQUAL /usr/local)
        set(CMAKE_INSTALL_PREFIX ${DSTORE_COMMON_DEFAULT_OUTPUT} CACHE STRING "install prefix" FORCE)
    endif()
else()
    set(CMAKE_INSTALL_PREFIX ${DSTORE_COMMON_DEFAULT_OUTPUT} CACHE STRING "install prefix" FORCE)
endif()

message(STATUS "install prefix : ${CMAKE_INSTALL_PREFIX}")
set(CMAKE_INSTALL_PUBLIC_HEAD_DIR ${CMAKE_INSTALL_PREFIX}/include)

if("$ENV{GCC_VERSION}" STREQUAL "")
    execute_process(COMMAND gcc --version OUTPUT_VARIABLE GCC_VERSION_INFO OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REPLACE "\n" " " GCC_VERSION_INFO_TMP ${GCC_VERSION_INFO})
    string(REPLACE " " ";" GCC_VERSION_INFO_LIST ${GCC_VERSION_INFO_TMP})
    list(GET GCC_VERSION_INFO_LIST 2 GCC_VERSION)
else()
    set(GCC_VERSION $ENV{GCC_VERSION})
endif()
if("${GCC_VERSION}" STREQUAL "7.3.0")
    set(GCC_VERSION_LIT 7.3)
endif()

set(PROJECT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/ CACHE INTERNAL "")

if(DEFINED ENV{DEBUG_TYPE})
    string(TOLOWER $ENV{DEBUG_TYPE} CMAKE_BUILD_TYPE_TMP)
endif()
if(CMAKE_BUILD_TYPE)
    string(TOLOWER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_TMP)
endif()
set(CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE_TMP})
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "debug" CACHE STRING "Default build type is debug" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "debug" "release")
endif()
message(STATUS "CMAKE_BUILD_TYPE is ${CMAKE_BUILD_TYPE}")

# Call cmake scripts
include(build_function)
include(set_lib_path)
include(build_options)

add_subdirectory(src)
add_subdirectory(tools)

if (ENABLE_TEST STREQUAL "ON")
    add_subdirectory("tests")
endif()

install(FILES
    ${UTILS_LIB_PATH}/libgsutils.so
    DESTINATION lib)
install(DIRECTORY ${LZ4_LIB_PATH} DESTINATION ${CMAKE_INSTALL_PREFIX}
    DIRECTORY_PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    FILE_PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    PATTERN "pkgconfig" EXCLUDE
    PATTERN "cmake" EXCLUDE
    PATTERN "*.a" EXCLUDE
    )

if (DSTORE_TEST_TOOL STREQUAL "OFF")
    install(TARGETS dstore LIBRARY DESTINATION lib)
    install(TARGETS dstore_static LIBRARY DESTINATION lib)
    install(FILES
            "${PROJECT_SOURCE_DIR}/build_script/dynamic_vfs.conf.init"
            "${PROJECT_SOURCE_DIR}/build_script/guc.json"
            DESTINATION share/postgresql)
    install(TARGETS
            pagedump
            waldump
            whatiserrcode
            htablookup
            DESTINATION bin)
    file(GLOB DSTORE_INTERFACE_SRC_DIR "${PROJECT_SOURCE_DIR}/interface/*")
    MESSAGE("remove ${PROJECT_SOURCE_DIR}/interface/*")
    MESSAGE("install ${DSTORE_INTERFACE_SRC_DIR}")
    file(MAKE_DIRECTORY "${CMAKE_INSTALL_PREFIX}/include")
    install(DIRECTORY
            ${DSTORE_INTERFACE_SRC_DIR}
            DESTINATION include/)
endif()

# get cmake relationship dependency diagram
add_custom_target(graphviz
    COMMAND rm -rf ${CMAKE_BINARY_DIR}/graph
    COMMAND mkdir -p ${CMAKE_BINARY_DIR}/graph
    COMMAND "${CMAKE_COMMAND}" "--graphviz=graph/dstore_graph" .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
)

if (${ENABLE_UT} STREQUAL "ON")
    set(RUN_TARGETS_SCRIPT "${CMAKE_BINARY_DIR}/run_targets.cmake")
    set(FIRST_CONFIGURE_RUN_ONCE "${CMAKE_BINARY_DIR}/first_configure_run_once.stamp")

    if(NOT EXISTS "${FIRST_CONFIGURE_RUN_ONCE}")
        message(STATUS "First-time configure: creating empty run_targets.cmake")
        configure_file(
            ${CMAKE_MODULE_PATH}/run_targets.in.cmake
            ${RUN_TARGETS_SCRIPT}
            COPYONLY
        )
        file(WRITE "${FIRST_CONFIGURE_RUN_ONCE}" "This file ensures we don't overwrite run_targets.cmake on reconfigure.")
    endif()

    include(${RUN_TARGETS_SCRIPT})

    install(CODE "
        message(STATUS \"Install completed, generating test targets...\")
        set(INSTALLED_BIN_DIR \"${CMAKE_BINARY_DIR}\")
        set(GROUP_STRATEGY ${GROUP_STRATEGY})
        set(RUN_TARGETS_SCRIPT \"${RUN_TARGETS_SCRIPT}\")
        include(\"${CMAKE_MODULE_PATH}/generate_run_targets.cmake\")
    ")
endif()
