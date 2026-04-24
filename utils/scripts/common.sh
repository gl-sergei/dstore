#!/bin/bash
# Copyright (C) 2026 Huawei Technologies Co.,Ltd.
#
# dstore is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# dstore is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. if not, see <https://www.gnu.org/licenses/>.

# Build CloudNativeDatabaseUtils repository.

set -e

declare LOG_FILE="${SCRIPT_DIR}/makeutils.log"
declare ARCHIVE_DIR=$LOCAL_LIB_PATH/archive
declare ERR_FAILED=1

function log()
{
    echo "[makeutils] $(date +%y-%m-%d' '%T): $@"
    echo "[makeutils] $(date +%y-%m-%d' '%T): $@" >> "$LOG_FILE" 2>&1
}

function die()
{
    log "$@"
    exit $ERR_FAILED
}

#######################################################################
# set buildtools
#######################################################################
gcc_version="7.3"
gcc_version_10="10.3"
ccache -V >/dev/null 2>&1 && USE_CCACHE="ccache " ENABLE_CCACHE="--enable-ccache"

sys_tools_upper=$(echo "${sys_tools}" | tr '[:lower:]' '[:upper:]')
if [ X"${sys_tools_upper}" = X"ON" ] &&  [ -n "${BISHENG_CPU_HOME}" ]; then
    export GCC_INSTALL_HOME="${BISHENG_CPU_HOME}"
    log "[INFO] GCC_INSTALL_HOME:${BISHENG_CPU_HOME}"
elif [ X"${sys_tools_upper}" = X"ON" ]; then
    export GCC_INSTALL_HOME=$(gcc -v 2>&1 | grep prefix | awk -F'prefix=' '{print $2}' |awk -F' ' '{print $1}')
else
    export GCC_INSTALL_HOME=$LOCAL_LIB_PATH/buildtools/gcc${gcc_version}/gcc
fi
if [ X"${sys_tools_upper}" != X"ON" ]; then
    export PATH=${GCC_INSTALL_HOME}/bin:${PATH}
fi
if [ ! -d "${GCC_INSTALL_HOME}" ]; then
    die "[ERROR] No gcc path"
fi
export CC="${USE_CCACHE}${GCC_INSTALL_HOME}/bin/gcc"
export CXX="${USE_CCACHE}${GCC_INSTALL_HOME}/bin/g++"
gcc_actual_version=$($CC -dumpversion | cut -d. -f1-2)
if [ "${gcc_actual_version}" != "${gcc_version}" ] && [ "${gcc_actual_version}" != "${gcc_version_10}" ]; then
    log "[WARNING] GCC version ${gcc_actual_version} is not officially supported (expected ${gcc_version} or ${gcc_version_10}), proceeding anyway"
fi

# cpu num
cpu_processor_num=$(grep processor /proc/cpuinfo | wc -l)
job_num=$(expr "$cpu_processor_num" \* 2)
log "[INFO] Compile parallel job num:${job_num}"

# build path
build_path="${ROOT_DIR}/tmp_build"