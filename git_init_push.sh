#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# git_init_push.sh
#
# One-time setup: turn this folder into a git repo, push it to GitHub under the
# murftech7@gmail.com account, and create a `dev` branch off main.
#
# Run it from the repo root:   bash git_init_push.sh
# Safe to re-read; NOT meant to be re-run once the remote exists.
# ---------------------------------------------------------------------------
set -euo pipefail

# --- 0. Settings you must confirm before running ---------------------------
GH_USER="murftech"                    # <-- your GitHub *username* (not email)
GH_EMAIL="murftech7@gmail.com"         # commit author email
REPO_NAME="macroecons"
VISIBILITY="private"                   # private | public
DEFAULT_BRANCH="main"
DEV_BRANCH="dev"

REPO_ROOT="/Users/murftech/Dropbox/Datarepo/macroecons"
cd "$REPO_ROOT"

echo "==> Repo root: $(pwd)"


# --- 1. Guard: don't re-init an existing repo ------------------------------
if [ -d .git ]; then
  echo "!!! .git already exists here. Stopping so nothing is clobbered."
  git remote -v
  exit 1
fi


# --- 2. .gitignore ---------------------------------------------------------
# .gitignore ships alongside this script and must be in place BEFORE the first
# `git add` — junk that enters the first commit stays in history forever.
# Edit .gitignore, not this section, when the exclusions need to change.
if [ ! -f .gitignore ]; then
  echo "!!! No .gitignore found in $(pwd)."
  echo "!!! It belongs next to this script. Copy it in, then re-run."
  exit 1
fi
echo "==> Using .gitignore ($(grep -cvE '^\s*(#|$)' .gitignore) active patterns)"


# --- 3. Init and make the first commit -------------------------------------
echo "==> git init"
git init -b "$DEFAULT_BRANCH"

# Set identity on this repo only, so your other repos keep their own email.
git config user.email "$GH_EMAIL"
git config user.name  "$GH_USER"

git add -A

# Sanity view BEFORE committing: anything here you didn't expect (venv, data,
# credentials) means fix .gitignore and re-run `git add -A` rather than commit.
echo "==> Files about to be committed (first 40):"
git status --short | head -40
echo "==> Total staged files: $(git diff --cached --name-only | wc -l | tr -d ' ')"

# Date is resolved at run time, so the template stays correct whenever it's used.
git commit -m "Initial commit $(date +%F)"


# --- 4. Create the GitHub repo and push ------------------------------------
# Option A (easiest) — GitHub CLI. Check you're on the right account first:
#   gh auth status
#   gh auth login          # if it shows the wrong account / not logged in
echo "==> Creating GitHub repo via gh"
gh repo create "$GH_USER/$REPO_NAME" \
  --"$VISIBILITY" \
  --source=. \
  --remote=origin \
  --push

# Option B — no gh: create the empty repo by hand at github.com/new
# (no README, no .gitignore — it must be empty or the push will be rejected),
# then:
#   git remote add origin git@github.com:$GH_USER/$REPO_NAME.git
#   git push -u origin $DEFAULT_BRANCH


# --- 5. Branch off for dev -------------------------------------------------
echo "==> Creating $DEV_BRANCH"
git checkout -b "$DEV_BRANCH"
git push -u origin "$DEV_BRANCH"      # -u sets upstream so later `git push` is bare


# --- 6. Where you ended up -------------------------------------------------
echo
echo "==> Done."
git remote -v
git branch -vv
echo "You are now on: $(git rev-parse --abbrev-ref HEAD)"
echo "Work on $DEV_BRANCH, merge into $DEFAULT_BRANCH via PR."
