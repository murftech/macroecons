

dump() {
  echo "  \$#  count   = $#"
  echo "  \$1          = ${1:-<none>}"
  echo "  \$2          = ${2:-<none>}"
  echo "  \$3          = ${3:-<none>}"
  echo "  \$4          = ${4:-<none>}"
  echo "  \$5          = ${5:-<none>}"
  echo "  \$@  all     = $@"
  local i=1
  for a in "$@"; do
    echo "        [$i] = $a"
    i=$((i + 1))
  done
}

echo "=== STAGE 0: as the script received them ============================"
dump "$@"

STEP="${1:-}"
echo
echo ">> STEP = '${STEP}'   <- \$1, saved before shift. the case picks a branch on this."

shift || true   # || true: shift fails when there's nothing to drop (zero args)
echo
echo "=== STAGE 1: after one shift (step name gone) ======================="
dump "$@"
echo
echo ">> this is what gets forwarded:   python some_step.py $@"

shift || true
echo
echo "=== STAGE 2: after another shift (just to show it slides again) ====="
dump "$@"


