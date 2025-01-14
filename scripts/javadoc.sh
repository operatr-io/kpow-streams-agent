#!/bin/bash

# Best practice shell options
set -euo pipefail
IFS=$'\n\t'

mkdir -p target/javadoc-out

# Run the javadoc command with the -quiet option
OUTPUT=`javadoc -quiet -classpath "$(lein classpath)" -Xdoclint:all -d target/javadoc-out $(find src -name "*.java") 2>&1`

# Check if there is any output
if [ -n "$OUTPUT" ]; then
  echo "Javadoc warnings or errors detected:"
  echo "$OUTPUT"
  exit 1
else
  echo "Javadoc completed successfully with no warnings or errors."
  exit 0
fi
