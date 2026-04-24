# configure CMake parameters
option(ENABLE_TEST "enable test" ON)
option(ENABLE_UT "enable unitest" OFF)
option(ENABLE_FUZZ "enable fuzztest" OFF)
option(ENABLE_LCOV "enable lcov coverage" OFF)
option(ENABLE_HCOV "enable hitest coverage" OFF)
option(ENABLE_TPCC "Integration Test" OFF)
option(ENABLE_JEPROF "enable jeprof, default OFF" OFF)
option(SUPPORT_HOTPATCH "support hotpatch, default OFF" OFF)
option(ENABLE_OBJDIFF "enable hotpatch objdiff" OFF)
# history has been named
option(MODE "mode to compatible previous debug mode" OFF)
option(UT "Unit Test" OFF)
option(DSTORE_TEST_TOOL "Integration Test" OFF)

option(PD_ENTRY_STATS_COLLECTION "Enabling collecting page access statistics" OFF)
option(USE_INTEGER_DATETIMES "enable integer datetimes" ON)
option(DSTORE_USE_ASSERT_CHECKING "enable cassert, the old is --enable-cassert" OFF)
option(__ARM_LSE "enable LSE Instruction" OFF)
option(ENABLE_LSE "enable lse,the old is --enable-lse" ON)

if (MODE STREQUAL "ASAN")
    set(ENABLE_MEMORY_CHECK ON)
endif()
if (MODE STREQUAL "TSAN")
    set(ENABLE_TSAN_CHECK ON)
endif()

if (${ENABLE_FUZZ} STREQUAL "ON")
    set(ENABLE_UT "ON")
endif()

if (${ENABLE_UT} STREQUAL "ON")
    set(UT "ON")
endif()

if (${ENABLE_TPCC} STREQUAL "ON" OR ${DSTORE_TEST_TOOL} STREQUAL "ON")
    set(ENABLE_TPCC "ON")
    set(DSTORE_TEST_TOOL "ON")
endif()

if (USE_ASSERT_CHECKING)
    set(DSTORE_USE_ASSERT_CHECKING ON)
endif()

if (USE_INTEGER_DATETIMES STREQUAL "OFF")
    set(USE_INTEGER_DATETIMES OFF)
endif()

# Required compile flags
set(PROTECT_OPTIONS -pthread -rdynamic)
set(WARNING_OPTIONS -Wall -Werror -fstack-protector -Wextra -Wno-error=cast-qual -Woverloaded-virtual
                    -Wtrampolines -Wformat=2 -Wdate-time -Wfloat-equal -Wswitch-default -Wunused -Wundef
                    -Wshadow -Wnon-virtual-dtor -Wdelete-non-virtual-dtor -Wundef -Wunused -Wcast-align
                    -Wcast-qual -Wvla -Wframe-larger-than=8192000 -Wstack-usage=8192000 -Wendif-labels
                    -Wno-register -Wno-error=address -Wno-error=invalid-memory-model)
set(OPTIMIZE_OPTIONS -fno-strict-aliasing -fno-common -freg-struct-return -fstrong-eval-order -pipe)
set(CHECK_OPTIONS "")
set(MACRO_OPTIONS "")

# libraries need link options during linking
set(LIB_LINK_OPTIONS -pthread -Wl,-z,noexecstack -Wl,-z,relro,-z,now -rdynamic)
set(LIB_SECURE_OPTIONS -fPIC -fstack-protector)

set(BIN_SECURE_OPTIONS -fstack-protector)
set(BIN_LINK_OPTIONS -pthread -Wl,-z,noexecstack -Wl,-z,relro,-z,now -rdynamic -Wl,--no-undefined)

# set options
if(CMAKE_BUILD_TYPE STREQUAL debug)
    if (NOT DSTORE_USE_ASSERT_CHECKING)
        set(DSTORE_USE_ASSERT_CHECKING ON)
    endif()
    list(APPEND PROTECT_OPTIONS -O0 -ggdb)
    list(APPEND OPTIMIZE_OPTIONS -fvisibility=default)
elseif(CMAKE_BUILD_TYPE STREQUAL release)
    set(SUPPORT_HOTPATCH ON)
    list(APPEND PROTECT_OPTIONS -O2 -g3)
    list(APPEND OPTIMIZE_OPTIONS -ffunction-sections -fdata-sections -Wl,-gc-sections -Wl,-print-gc-sections -fvisibility=hidden -fvisibility-inlines-hidden)
elseif(CMAKE_BUILD_TYPE STREQUAL coverage)
    set(SUPPORT_HOTPATCH ON)
    list(APPEND PROTECT_OPTIONS -O2 -g3)
    list(APPEND OPTIMIZE_OPTIONS -fvisibility=hidden -fvisibility-inlines-hidden)
    set(ENABLE_LCOV ON)
else()
    list(APPEND PROTECT_OPTIONS -O0 -g)
    list(APPEND OPTIMIZE_OPTIONS -fvisibility=hidden -fvisibility-inlines-hidden)
endif()

if(${ENABLE_UT} STREQUAL "ON" OR ${ENABLE_LCOV} STREQUAL "ON")
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
    # UT test need change binaries to libraries,set -fPIC during compling
    list(APPEND BIN_SECURE_OPTIONS -fPIC)
else()
    # libraries need secure options during compling
    list(APPEND BIN_SECURE_OPTIONS -fPIE)
    # binaries need fPIE pie link options during linking
    list(APPEND BIN_LINK_OPTIONS -pie)
endif()

if (ENABLE_UT STREQUAL "ON" OR DSTORE_TEST_TOOL STREQUAL "ON" OR CMAKE_BUILD_TYPE STREQUAL "tsancheck")
    list(APPEND OPTIMIZE_OPTIONS -fvisibility=default)
endif()

if (DEFINED ENV{GS_USE_NEW_CONFIG_METHOD})
    list(APPEND MACRO_OPTIONS GS_NEW_CONFIG_METHOD=ON)
endif()

if (DEFINED ENV{GS_ON_DEMAND_PAGE_REPLAY})
    list(APPEND MACRO_OPTIONS ON_DEMAND_PAGE_REPLAY=ON)
endif()

if (DEFINED ENV{GS_RECOVERY_PERF_COLLECTION})
    list(APPEND MACRO_OPTIONS RECOVERY_PERF_COLLECTION=ON)
endif()

if (DEFINED ENV{GS_BUFFERPOOL_DEBUG})
    list(APPEND MACRO_OPTIONS BUFFERPOOL_DEBUG=ON)
endif()

if (DEFINED ENV{GS_BUFFERPOOL_SYNC_LOCK})
    list(APPEND MACRO_OPTIONS BUFFERPOOL_SYNC_LOCK=ON)
endif()

if (DEFINED ENV{GS_LOCK_DEBUG} OR ${ENABLE_UT} STREQUAL "ON")
    list(APPEND MACRO_OPTIONS LOCK_DEBUG=ON)
endif()

if(${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "x86_64")
    list(APPEND PROTECT_OPTIONS -msse4.2 -mcx16)
elseif(${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "aarch64")
    if (CMAKE_BUILD_TYPE AND (CMAKE_BUILD_TYPE STREQUAL "release") AND ${ENABLE_LSE} STREQUAL "ON")
        list(APPEND PROTECT_OPTIONS -march=armv8-a+crc+lse)
        set(__ARM_LSE ON)
    else()
        list(APPEND PROTECT_OPTIONS -march=armv8-a+crc)
    endif()
endif()

set(TEST_LINK_DIRECTORIES "")
set(MEMCHECK_LINK_LIBS "")
# set memcheck compile options
if(${ENABLE_MEMORY_CHECK})
    list(APPEND MACRO_OPTIONS -DMEMCHECK)
    set(DSTORE_USE_ASSERT_CHECKING ON)
    # -fno-sanitize=vptr: forbide detection of certain conversions between pointers to base and derived classes
    list(APPEND CHECK_OPTIONS -fsanitize=address,leak,undefined -fno-omit-frame-pointer -fno-sanitize=vla-bound -fno-sanitize=vptr -fno-sanitize=nonnull-attribute -fno-sanitize=alignment -fsanitize-recover=undefined)
    list(APPEND TEST_LIMEMCHECK_LINK_LIBSNK_LIBS -static-libasan -static-libubsan dl m rt)
    list(APPEND LIB_LINK_OPTIONS -fsanitize=address,leak,undefined -fsanitize-recover=undefined)
    list(APPEND BIN_LINK_OPTIONS -fsanitize=address,leak,undefined -fsanitize-recover=undefined)
    list(REMOVE_ITEM LIB_SECURE_OPTIONS -fstack-protector)
    list(REMOVE_ITEM BIN_SECURE_OPTIONS -fstack-protector)
    if("${GCC_VERSION}" STREQUAL "10.3.1")
        list(APPEND MEMCHECK_LINK_LIBS stdc++)
    endif()
    list(APPEND TEST_LINK_DIRECTORIES ${MEMCHECK_LIB_PATH})
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
    list(REMOVE_ITEM WARNING_OPTIONS -fstack-protector)
endif()

add_definitions(-Wno-builtin-macro-redefined)

# set tsancheck compile options
if(${ENABLE_TSAN_CHECK})
    list(APPEND MACRO_OPTIONS -D__SANITIZE_THREAD__ -DENABLE_THREAD_CHECK -D_REENTRANT)
    list(APPEND CHECK_OPTIONS -fsanitize=thread)
    list(APPEND LIB_LINK_OPTIONS -static-libtsan)
    list(APPEND BIN_LINK_OPTIONS -static-libtsan)
    list(APPEND MEMCHECK_LINK_LIBS libtsan.a dl m rt)
    list(APPEND TEST_LINK_DIRECTORIES ${MEMCHECK_LIB_PATH})
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
    list(REMOVE_ITEM WARNING_OPTIONS -fstack-protector)
endif()

set(TEST_LINK_LIBS "")
if(ENABLE_LCOV STREQUAL "ON")
    list(APPEND CHECK_OPTIONS -fprofile-arcs -ftest-coverage )
    list(APPEND TEST_LINK_LIBS -lgcov)
    add_definitions(-D ENABLE_LCOV)
    message(STATUS "Enable lcov coverage")
endif()

if(${ENABLE_HCOV} STREQUAL "ON")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE hitestwrapper)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK hitestwrapper)
    add_definitions(-D ENABLE_HCOV)
    message(STATUS "Enable hitest coverage")
endif()

if (ENABLE_UT STREQUAL "ON")
    list(APPEND MACRO_OPTIONS -DENABLE_FAULT_INJECTION=1)
    option(DISABLE_TABLESPACE_MOCK "OFF means using tablespace mock implementation" OFF)
    option(DISABLE_SEGMENT_MOCK "OFF means using segment mock implementation" OFF)
    option(DISABLE_BUFFER_MOCK "OFF means using buffer mock implementation" OFF)
    option(DISABLE_UNDO_MOCK "OFF means using undo mock implementation" OFF)
    set(UT_LIBS gmock_maind gtestd gmockd)
    if(${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "x86_64")
        list(APPEND MACRO_OPTIONS -D_GLIBCXX_USE_CXX11_ABI=0)
        set(UT_LIBS gmock_main gtest gmock)
    endif()
endif()

message(STATUS "UT MODE: ${MODE}")
message(STATUS "CMAKE_HOST_SYSTEM_PROCESSOR:${CMAKE_HOST_SYSTEM_PROCESSOR}")

configure_file (
  "${PROJECT_SOURCE_DIR}/cmake/config.h.in"
  "${PROJECT_SOURCE_DIR}/include/config.h"
)


#objdiff
if(${ENABLE_OBJDIFF} STREQUAL "ON")
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
    list(REMOVE_ITEM LIB_SECURE_OPTIONS -fPIC)
    list(APPEND CHECK_OPTIONS -fno-extern-tls-init -ffunction-sections -D__LINE__=0 -fno-PIE -fPIC -Wa,--compress-debug-sections=none -fdata-sections -fno-section-anchors -fno-common)
    if(${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "x86_64")
        list(APPEND CHECK_OPTIONS -mcmodel=large -Wa,-mrelax-relocations=no)
    endif()
endif()
