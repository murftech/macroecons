#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# git_teardown.sh
#
# Undo git_init_push.sh: remove version control from this folder so you can
# start over. Use when the repo was initialised wrong — bad account, junk in
# the first commit, wrong remote, committed secrets.
#
#   bash git_teardown.sh
#
# WHAT IT TOUCHES:  the .git/ directory only.
# WHAT IT NEVER TOUCHES:  your actual files. Code, data and configs are left
# exactly as they are — removing .git makes them ordinary untracked files again.
#
# By default .git is ARCHIVED to the parent directory before deletion, so a
# mistake is recoverable. See section 5 for how to restore, and FORCE_DELETE
# for the unrecoverable version.
# ---------------------------------------------------------------------------
set -euo pipefail

REPO_ROOT="/Users/murftech/Dropbox/Datarepo/macroecons"
REPO_NAME="macroecons"
GH_USER="murftech"

# Set to 1 to skip the archive and hard-delete .git. Only do this when you are
# certain there is nothing in history you want back.
FORCE_DELETE="${FORCE_DELETE:-0}"

cd "$REPO_ROOT"
echo "==> Target: $(pwd)"


# --- 1. Guard: is there anything to tear down? -----------------------------
if [ ! -d .git ]; then
  echo "==> No .git directory here. Nothing to do."
  exit 0
fi

# Guard against running this from a subdirectory of a *different* repo, or from
# somewhere unexpected: the .git we found must belong to this exact folder.
GIT_TOPLEVEL="$(git rev-parse --show-toplevel)"
if [ "$GIT_TOPLEVEL" != "$(pwd -P)" ]; then
  echo "!!! The .git in scope belongs to: $GIT_TOPLEVEL"
  echo "!!! That is not this folder. Stopping rather than deleting someone"
  echo "!!! else's repository."
  exit 1
fi


# --- 2. Show what is about to be lost --------------------------------------
# Everything below is read-only. Read it before answering the prompt — this is
# the whole point of the script, the deletion itself is one line.
echo
echo "--------------------------------------------------------------------"
echo "REMOTES (where this repo pushes to)"
git remote -v || echo "  (none)"

echo
echo "BRANCHES"
git branch -vv || echo "  (none yet)"

echo
echo "COMMITS (last 10)"
git --no-pager log --oneline -10 2>/dev/null || echo "  (no commits yet)"

echo
echo "UNCOMMITTED CHANGES (these survive teardown — they are just files)"
git status --short | head -20
echo "  total: $(git status --porcelain | wc -l | tr -d ' ') paths"

echo
echo "STASHES (these are stored INSIDE .git and WILL be lost)"
git stash list || true
echo "  total: $(git stash list | wc -l | tr -d ' ')"

echo
echo "UNPUSHED COMMITS (exist only here — lost unless archived)"
# Compare each local branch against its upstream, if it has one.
UNPUSHED=0
for BRANCH in $(git for-each-ref --format='%(refname:short)' refs/heads/); do
  if git rev-parse --abbrev-ref "$BRANCH@{upstream}" >/dev/null 2>&1; then
    AHEAD="$(git rev-list --count "$BRANCH@{upstream}..$BRANCH")"
    echo "  $BRANCH: $AHEAD commit(s) ahead of upstream"
    UNPUSHED=$((UNPUSHED + AHEAD))
  else
    COUNT="$(git rev-list --count "$BRANCH")"
    echo "  $BRANCH: no upstream — all $COUNT commit(s) are local only"
    UNPUSHED=$((UNPUSHED + COUNT))
  fi
done
echo "--------------------------------------------------------------------"
echo


# --- 3. Explicit confirmation ----------------------------------------------
if [ "$UNPUSHED" -gt 0 ]; then
  echo "!!! $UNPUSHED commit(s) are not on any remote."
fi
if [ "$FORCE_DELETE" = "1" ]; then
  echo "!!! FORCE_DELETE=1 — .git will be deleted with NO archive. Unrecoverable."
fi

printf 'Type the repo name (%s) to proceed, anything else to abort: ' "$REPO_NAME"
read -r CONFIRM
if [ "$CONFIRM" != "$REPO_NAME" ]; then
  echo "==> Aborted. Nothing changed."
  exit 1
fi


# --- 4. Archive, then remove -----------------------------------------------
if [ "$FORCE_DELETE" != "1" ]; then
  STAMP="$(date +%Y%m%d-%H%M%S)"
  ARCHIVE="../${REPO_NAME}-git-backup-${STAMP}.tar.gz"
  echo "==> Archiving .git to $ARCHIVE"
  # Archived to the PARENT dir on purpose: if it sat inside the repo, a later
  # `git add -A` would sweep the backup into the new history.
  tar -czf "$ARCHIVE" .git
  echo "==> Archive size: $(du -h "$ARCHIVE" | cut -f1)"
fi

echo "==> Removing .git"
rm -rf .git

echo "==> Verifying"
if [ -d .git ]; then
  echo "!!! .git still present — check permissions."
  exit 1
fi
git status 2>&1 | head -2   # expect: "not a git repository"


# --- 5. Where you ended up -------------------------------------------------
echo
echo "==> Done. Files are untouched and now untracked."
echo "    Working tree still has $(ls -A | wc -l | tr -d ' ') entries."
if [ "$FORCE_DELETE" != "1" ]; then
  echo
  echo "    To restore history:   tar -xzf ${ARCHIVE} -C ."
  echo "    To discard it later:  rm ${ARCHIVE}"
fi
echo "    To start fresh:       bash git_init_push.sh"


# --- 6. The remote is still there ------------------------------------------
# This script is LOCAL ONLY. github.com/$GH_USER/$REPO_NAME still exists with
# everything you already pushed — including anything you are tearing down to
# get rid of. Deleting it is irreversible and cannot be undone by any script
# here, so it is left to you, deliberately:
#
#   gh repo view   $GH_USER/$REPO_NAME     # confirm what you are looking at
#   gh repo delete $GH_USER/$REPO_NAME     # prompts for confirmation
#
# NOTE ON SECRETS: if a credential was ever pushed, deleting the repo is not
# enough. Treat it as compromised and rotate the key — it may already have been
# scraped, cached by GitHub, or forked.
echo
echo "    Remote NOT deleted: github.com/$GH_USER/$REPO_NAME (see section 6)"
