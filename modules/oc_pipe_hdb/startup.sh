#!/bin/bash
set -e

cd "$(dirname "$0")"

usage() {
  echo "Usage: $0 [install|refresh]"
  echo "  install  - install dependencies from frozen uv.lock"
  echo "  refresh  - regenerate uv.lock from pyproject.toml"
  exit 1
}

ACTION="$1"

case "$ACTION" in
  install)
    echo "📦 Installing dependencies..."
    echo "Running uv sync"
    uv sync --frozen
    ;;
  refresh)
    echo "🔄 Refreshing uv.lock..."
    echo "Running uv lock"
    uv lock
    ;;
  *)
    usage
    ;;
esac


# Please install uv on your mac or windows if you have not already.
# please do not worry, it can be reversed out of your system

# if Mac:
# if ! command -v uv &> /dev/null; then
#   brew install uv
# fi

# reversal:
# uv cache clean
# uv python uninstall --all
# brew uninstall uv

# if Windows:
# winget install --id=astral-sh.uv
