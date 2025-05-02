#!/bin/bash

LINTER_PATH="$GOPATH/bin/golangci-lint"

## if go isn't found, exit
if ! command -v go > /dev/null 2>&1; then
  echo "Go is not installed. Please install Go and try again."
  exit 1
else
  go version
fi

# Ensure the lint tool is installed
if [ ! -f "$LINTER_PATH" ]; then
  echo "GolangCI Lint not found. Installing..."
  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.64.5
else
  echo "GolangCI Lint is already installed."
  $LINTER_PATH --version
fi

# Run linter
$LINTER_PATH run
