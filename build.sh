#!/bin/bash
#
# Script for Dev's daily work. It uses the same build options as the released version.

set -euo pipefail

# --- Helper Functions ---

version_ge() {
    [[ "$(printf '%s\n' "$@" | sort -rV | head -n1)" == "$1" ]]
}

get_mach_type() {
    mach_type=$(uname -m)
}

get_os_type() {
    os_type=$(uname)
    if [[ "$os_type" == MINGW* ]]; then
        os_type="WIN"
    fi
}

get_linux_version() {
    local kernel=$(uname -r)
    case "$kernel" in
        *el6*|*alios6*) linux_version="alios6" ;;
        *el7*|*alios7*) linux_version="alios7" ;;
        *el8*|*alios8*|*al8*) linux_version="alios8" ;;
        *) linux_version="not_alios" ;;
    esac
}

get_key_value() {
    echo "${1#*=}"
}

usage() {
cat <<EOF
Usage: $0 [-t debug|release] [-d <dest_dir>] [-s <server_suffix>] [-g asan|tsan]
       Or
       $0 [-h | --help]

  -t                      Build type: debug or release (default: debug)
  -d                      Destination directory (default: /usr/local/alisql or \$HOME/alisql)
  -s                      Server suffix (default: alisql-dev)
  -g                      Sanitizer: asan or tsan
  -c                      Enable GCC coverage (gcov)
  -p                      Enable perf-related code
  -h, --help              Show this help

EOF
}

parse_options() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -t=*) build_type="${1#*=}" ;;
            -t)   shift; build_type="$1" ;;
            -d=*) dest_dir="${1#*=}" ;;
            -d)   shift; dest_dir="$1" ;;
            -s=*) server_suffix="${1#*=}" ;;
            -s)   shift; server_suffix="$1" ;;
            -g=*) san_type="${1#*=}" ;;
            -g)   shift; san_type="$1" ;;
            -c=*) enable_gcov="${1#*=}" ;;
            -c)   shift; enable_gcov="$1" ;;
            -h|--help) usage; exit 0 ;;
            *) echo "Unknown option: '$1'" >&2; exit 1 ;;
        esac
        shift
    done
}

dump_options() {
    cat <<EOF
=== Build Configuration ===
build_type:      $build_type
dest_dir:        $dest_dir
server_suffix:   $server_suffix
san_type:        $san_type
enable_gcov:     $enable_gcov
mach_type:       $mach_type
os_type:         $os_type
linux_version:   $linux_version
CC:              $CC
CXX:             $CXX
CFLAGS:          $CFLAGS
CXXFLAGS:        $CXXFLAGS
===========================
EOF
}

# --- Main Logic ---

if [[ ! -f sql/mysqld.cc ]]; then
    echo "Error: Must run from MySQL top-level directory." >&2
    exit 1
fi

# Default values
build_type="debug"
if [[ -w /usr/local ]]; then
    dest_dir="/usr/local/alisql"
else
    dest_dir="$HOME/alisql"
fi
server_suffix="alisql-dev"
san_type=""
enable_gcov=0

# Parse CLI args
parse_options "$@"

# Detect system info
get_mach_type
get_os_type
get_linux_version

# Normalize build type
case "$build_type" in
    debug)
        cmake_build_type="Debug"
        debug=1
        gcov=$(( enable_gcov ))
        ;;
    release)
        cmake_build_type="RelWithDebInfo"
        debug=0
        gcov=0
        ;;
    *)
        echo "Invalid build type: '$build_type'. Must be 'debug' or 'release'." >&2
        exit 1
        ;;
esac

server_suffix="-$server_suffix"

# Set compiler flags
if [[ "$cmake_build_type" == "RelWithDebInfo" ]]; then
    COMMON_FLAGS="-O3 -g -fexceptions -fno-strict-aliasing"
elif [[ "$cmake_build_type" == "Debug" ]]; then
    COMMON_FLAGS="-O0 -g3 -gdwarf-2 -fexceptions -fno-strict-aliasing"
fi

# Architecture-specific flags
case "$mach_type" in
    x86_64)
        COMMON_FLAGS+=" -fno-omit-frame-pointer -D_GLIBCXX_USE_CXX11_ABI=0"
        ;;
    aarch64)
        COMMON_FLAGS+=" -mcpu=cortex-a72 -march=armv8-a+crypto+crc+lse -Wl,-Bsymbolic"
        ;;
    *)
        echo "Warning: Unknown machine type '$mach_type', using generic flags." >&2
        ;;
esac

# Sanitizer handling
asan=0; tsan=0
if [[ -n "$san_type" ]]; then
    case "$san_type" in
        asan)
            asan=1
            gcov=0
            ;;
        tsan)
            tsan=1
            gcov=0
            ;;
        *)
            echo "Invalid sanitizer: '$san_type'. Use 'asan' or 'tsan'." >&2
            exit 1
            ;;
    esac
fi

# Compiler detection: use system default unless already set
: ${CC:=$(which gcc 2>/dev/null || echo "gcc")}
: ${CXX:=$(which g++ 2>/dev/null || echo "g++")}

export CC CXX
CFLAGS="$COMMON_FLAGS"
CXXFLAGS="$COMMON_FLAGS"
export CFLAGS CXXFLAGS

gcc_version=$($CC --version | awk 'NR==1{print $3}')
cmake_version=$(cmake --version | awk 'NR==1{print $3}')

# Cleanup CMake cache only (avoid make clean which may be dangerous)
rm -f CMakeCache.txt
rm -rf CMakeFiles/

# Dump config
dump_options

# Shared CMake args
cmake_args=(
    -DFORCE_INSOURCE_BUILD=ON
    -DCMAKE_BUILD_TYPE="$cmake_build_type"
    -DMINIMAL_RELWITHDEBINFO=0
    -DSYSCONFDIR="$dest_dir"
    -DCMAKE_INSTALL_PREFIX="$dest_dir"
    -DMYSQL_DATADIR="$dest_dir/data"
    -DWITH_UNIT_TESTS=0
    -DWITH_DEBUG=$debug
    -DENABLE_GCOV=$gcov
    -DINSTALL_LAYOUT=STANDALONE
    -DMYSQL_MAINTAINER_MODE=0
    -DWITH_EMBEDDED_SERVER=0
    -DWITH_EXTRA_CHARSETS=all
    -DWITH_ZLIB=bundled
    -DWITH_ZSTD=bundled
    -DWITH_MYISAM_STORAGE_ENGINE=1
    -DWITH_INNOBASE_STORAGE_ENGINE=1
    -DWITH_CSV_STORAGE_ENGINE=1
    -DWITH_ARCHIVE_STORAGE_ENGINE=1
    -DWITH_BLACKHOLE_STORAGE_ENGINE=1
    -DWITH_FEDERATED_STORAGE_ENGINE=1
    -DWITH_PERFSCHEMA_STORAGE_ENGINE=1
    -DWITH_EXAMPLE_STORAGE_ENGINE=0
    -DWITH_TEMPTABLE_STORAGE_ENGINE=1
    -DDEFAULT_CHARSET=utf8mb4
    -DDEFAULT_COLLATION=utf8mb4_0900_ai_ci
    -DENABLED_PROFILING=1
    -DENABLED_LOCAL_INFILE=1
    -DWITH_ASAN=$asan
    -DWITH_TSAN=$tsan
    -DWITH_BOOST="extra/boost/boost_1_77_0"
    -DMYSQL_SERVER_SUFFIX="$server_suffix"
)

# Run CMake
cmake . "${cmake_args[@]}"

# Parallel build: use nproc if available, fallback to /proc/cpuinfo
if command -v nproc >/dev/null 2>&1; then
    JOBS=$(nproc)
else
    JOBS=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 4)
fi

make -j"$JOBS"

make_result=$?

exit "$make_result"
