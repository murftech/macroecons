#!/bin/bash
set -e

# forces cwd to the repo root regardless of where this script is invoked from - docker build
cd "$(dirname "$0")/.."


#######
# Install official java temurin
# version 17 is the widely adopted in cloud as of 2026 08
########

brew install --cask temurin@17

/usr/libexec/java_home -V
java -version 
ls /Library/Java/JavaVirtualMachines/

brew uninstall --cask temurin@17


# note: oh thats all? in the past i kept going website and all and doing all shit walao eh.



uv lock
uv lock --check
uv sync --frozen


uv run --with ipython ipython