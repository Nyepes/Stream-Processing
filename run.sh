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
    python src/mp2/failure_detector.py "$@"
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
else
  echo "Usage: $0 {dgrep|failure_detector}"
  exit 1
fi
