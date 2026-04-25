# add the directories you want to sift out from the whole UT suite
SET(ut_filter_list "")
list(APPEND ut_filter_list "include")

SET(unittest_src_list "" src/ut_wal/ut_wal_stream.cpp)
# Please make sure to only put cpp files in src folder
if (ENABLE_FUZZ STREQUAL "ON")
    FILE(GLOB_RECURSE unittest_src_list RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.cpp *.c)
else()
    FILE(GLOB_RECURSE unittest_src_list RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.cpp)
endif()

FILE(GLOB_RECURSE UT_EXCLUDE_TEMPLATE_SRC RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} */*/cct/template/*.cpp)
LIST(REMOVE_ITEM unittest_src_list ${UT_EXCLUDE_TEMPLATE_SRC})
message(STATUS "${UT_EXCLUDE_TEMPLATE_SRC}")

if (ENABLE_FUZZ STREQUAL "OFF")
    FILE(GLOB_RECURSE UT_EXCLUDE_FUZZ_SRC RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} fuzz_src/*.cpp)
    LIST(REMOVE_ITEM unittest_src_list ${UT_EXCLUDE_FUZZ_SRC})
    message(STATUS "EXCLUDE SRC " ${UT_EXCLUDE_FUZZ_SRC})
endif()

FILE(GLOB_RECURSE UT_EXCLUDE_FLASHBACK RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} */ut_flashback/*.cpp)
LIST(REMOVE_ITEM unittest_src_list ${UT_EXCLUDE_FLASHBACK})
message(STATUS "EXCLUDE SRC " ${UT_EXCLUDE_FLASHBACK})

message(STATUS "${unittest_src_list}")

set(CMAKE_SKIP_RPATH FALSE)

set(VFSCONFIGPATH ${UTILS_LIB_PATH}/libvfslinuxadapter.so)
message(STATUS "VFSCONFIGPATH is ${VFSCONFIGPATH}")
execute_process(COMMAND sed -i "s#type = PageStore#type = TenantIsolation#" ${PROJECT_SOURCE_DIR}/build_script/dynamic_vfs.conf.init)
execute_process(COMMAND sed -i "s#vfsLibPath = [0-9_a-zA-Z/$.]*$#vfsLibPath = ${VFSCONFIGPATH}#" ${PROJECT_SOURCE_DIR}/build_script/dynamic_vfs.conf.init)
execute_process(COMMAND sed -i "s#vfsName = GaussPageStore#vfsName = GaussTenantIsolation#" ${PROJECT_SOURCE_DIR}/build_script/dynamic_vfs.conf.init)
execute_process(COMMAND sed -i "s#vfsClusterName = PageStoreCluster#vfsClusterName = cluster#" ${PROJECT_SOURCE_DIR}/build_script/dynamic_vfs.conf.init)

# Add a custom target for VFS lib testing.
add_custom_target(vfs_config
    DEPENDS "${PROJECT_SOURCE_DIR}/build_script/dynamic_vfs.conf.init"
    COMMAND cp "${PROJECT_SOURCE_DIR}/build_script/dynamic_vfs.conf.init" ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
# Add a custom target for VFS lib testing.
add_custom_target(start_config
    DEPENDS "${PROJECT_SOURCE_DIR}/build_script/start_config.json"
    COMMAND cp "${PROJECT_SOURCE_DIR}/build_script/start_config.json" ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

execute_process(COMMAND sed -i "s#\"clientLibPath\":\\s*\"[^\"]*\"#\"clientLibPath\": \"${VFSCONFIGPATH}\"#" ${PROJECT_SOURCE_DIR}/tests/unittest/ut_config/ut_isoland_start.json)

set(STARTCONFIGPATH ${PROJECT_SOURCE_DIR}/tests/unittest/ut_config/ut_isoland_start.json)
execute_process(COMMAND sed -i "s#\"startConfigPath\":\\s*\"[^\"]*\"#\"startConfigPath\": \"${STARTCONFIGPATH}\"#" ${PROJECT_SOURCE_DIR}/build_script/guc.json)

set(unittest_compile_options ${PROTECT_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS})
set(unittest_link_options ${BIN_LINK_OPTIONS})
set(unittest_macro_options ${MACRO_OPTIONS})
set(unittest_include_directories
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    include
    ${GTEST_INCLUDE_PATH}
    ${MOCKCPP_INCLUDE_PATH}
    ${MOCKCPP_3RDPARTY_PATH}
)

if (ENABLE_FUZZ STREQUAL "ON")
    LIST(APPEND unittest_include_directories Secodefuzz Secodefuzz/common fuzz_include)
endif()

set(unittest_link_directories
    ${GTEST_LIB_PATH} ${MOCKCPP_LIB_PATH} ${LIBEVENT_LIB_PATH} ${SECURE_LIB_PATH}
)

# This is the whole unit test suite except dstore embedded dir
set(unittest_link_libs utilities gsutils ${UT_LIBS} mockcpp dstore securec -lrt -rdynamic -lstdc++ m)
add_bintarget(unittest unittest_src_list unittest_include_directories "${unittest_macro_options}" "${unittest_compile_options}" "${unittest_link_options}" "${unittest_link_libs}")
target_link_directories(unittest PRIVATE ${unittest_link_directories})
add_dependencies(unittest dstore utilities vfs_config start_config)

install(FILES
    ut_config/config
    ut_config/rdma_config
    ../../build_script/guc.json
    ut_config/ut_isoland_start.json
    ut_config/ssl_path/server.crt
    DESTINATION ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
)
