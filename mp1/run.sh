# Function to display usage
usage() {
    echo "Usage: $0 {build|server|client|test}"
    exit 1
}

# Function to build the project
build() {
    echo "Building the project..."
    pip install -r build/requirements.txt
}

# Function to test the project
test() {
    echo "Testing the project..."
    pytest
}

if [ $# -eq 0 ]; then
    usage
fi

# Handle the different options
case "$1" in
    build)
        build
        ;;
    test)
        test
        ;;
    *)
        usage
        ;;
esac