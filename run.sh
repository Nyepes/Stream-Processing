#!/bin/bash

# dgrep function: Searches for a string in a given directory recursively
dgrep_server() {
    python src/mp1/server.py "$@" 
}
dgrep() {
    python src/mp1/client.py "$@"
}

# failure_detector function: Simulates a simple check for failures
failure_detector() {
  python src/FailureDetector/failure_detector.py "$@"
}

start() {
  killall "python" 2> /dev/null &
  rm src/member_list.txt
  touch src/member_list.txt
  dgrep_server "$@" 2> /dev/null &
  failure_detector "$@" > /dev/null &
  python src/FileSystem/file_system.py "$@" &
  if [ "$1" == "1" ]; then
    python src/Streaming/leader.py "$@" > /dev/null&
  fi
  python src/Streaming/worker.py > /dev/null "$@" 
}

members() {
  python src/FailureDetector/utils/list_members.py
}

ls() {
  python src/FileSystem/bin/ls.py "$@"
}

store() {
  python src/FileSystem/bin/store.py "$@"
}

get_id() {
  python "src/FailureDetector/utils/get_id.py"
}

toggle_sus() {
  python "src/FailureDetector/utils/toggle_suspicion.py"
}

toggle_print_sus() {
  python "src/FailureDetector/utils/toggle_print_suspicion.py"
}

sus_status() {
  python "src/FailureDetector/utils/suspicion_status.py"
}

leave() {
  python "src/FailureDetector/utils/leave.py"
}

merge() {
   python "src/FileSystem/bin/merge.py"
}

build() {
    export PYTHONPATH="$PYTHONPATH:/src"
    python -m ensurepip --default-pip
    pip install -r build/requirements.txt
}

# Call the functions based on arguments
export PYTHONPATH="$PYTHONPATH:/src"

if [ "$1" == "dgrep_server" ]; then
    shift
    dgrep_server "$@"
elif [ "$1" == "dgrep" ]; then
  shift
  dgrep "$@"
elif [ "$1" == "failure_detector" ]; then
  shift
  failure_detector "$@"
elif [ "$1" == "build" ]; then
  build
elif [ "$1" == "start" ]; then
  shift
  rm -r src/FileSystem/fs
  mkdir -p src/FileSystem/fs/metadata
  rm -r src/FileSystem/local_cache
  mkdir -p src/FileSystem/local_cache/metadata
  killall "python"
  start "$@"
elif [ "$1" == "list_mem" ]; then
  members "$@"
elif [ "$1" == "list_self" ]; then
  get_id "$@"
elif [ "$1" == "toggle_sus" ]; then
  toggle_sus "$@"
elif [ "$1" == "sus_status" ]; then
  sus_status "$@"
elif [ "$1" == "toggle_print_sus" ]; then
  toggle_print_sus "$@"
elif [ "$1" == "leave" ]; then
  leave "$@"
elif [ "$1" == "get" ]; then
  shift 
  python src/FileSystem/bin/get_file.py "$@"
elif [ "$1" == "create" ]; then
  shift 
  echo "hello"
  python src/FileSystem/bin/create_file.py "$@"
elif [ "$1" == "append" ]; then
  shift 
  python src/FileSystem/bin/append.py "$@"
elif [ "$1" == "merge" ]; then
  shift 
  python src/FileSystem/bin/merge.py "$@"
elif [ "$1" == "ls" ]; then
  shift
  ls "$@"
elif [ "$1" == "store" ]; then
  shift
  store "$@"
elif [ "$1" == "getfromreplica" ]; then
  shift
  python src/FileSystem/bin/get_from_replica.py "$@"
elif [ "$1" == "list_mem_ids" ]; then
  members "$@"
elif [ "$1" == "multiappend" ]; then
  shift
  python src/FileSystem/bin/multiappend.py "$@"

elif [ "$1" == "reset_fs" ]; then
  shift
  rm -r src/FileSystem/fs
  mkdir -p src/FileSystem/fs/metadata

elif [ "$1" == "Rainstorm" ]; then
  shift
  python src/Streaming/Rainstorm.py "$@"
else
  echo "$1"
  echo "Command not found"
  exit 1
fi
