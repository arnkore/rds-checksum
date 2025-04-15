#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

rm -rf ./output
mkdir -pv ./output

# Define the output binary name
OUTPUT_BINARY="./output/rds-checksum"

# Define the main package path
MAIN_PACKAGE="./cmd/checksum/main.go"

echo "Building ${OUTPUT_BINARY}..."

# Build the Go application
# -o specifies the output file name
# -v prints the names of packages as they are compiled
go build -v -o "${OUTPUT_BINARY}" "${MAIN_PACKAGE}"

echo "Build successful! Binary created: ${OUTPUT_BINARY}"

# Optional: Make the script executable after creation
# chmod +x build.sh 