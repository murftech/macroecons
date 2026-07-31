#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

####################################################################
# WHAT THIS IS
#
# A runbook for collapsing dev's bootstrap commits into a smaller,
# readable set before promoting to main. Run ONE subcommand at a
# time, in the order listed by usage() - this is deliberately not a
# single "do it all" script, because step 3 opens an editor and step
# 5 is a judgement call you have to make with your own eyes.
#
#   sh git_squash.sh status     <- start here
####################################################################

####################################################################
# THE ONE SAFETY RULE
#
# You may rewrite any commit that exists ONLY on your branch.
# You may not rewrite a commit that main (or any other clone) has.
#
# The boundary is the merge-base:
#
#   git merge-base main dev     -> 2db6f22 "Initial commit"
#
# Everything ABOVE that is yours to reshape. 2db6f22 and below are
# on main AND origin/main - rewriting them changes every SHA above
# them and breaks any clone holding the old ones.
#
# "git log main..dev" is literally "commits I am allowed to touch".
# That range is the answer to "where do I squash from".
####################################################################

BASE=main
BRANCH=dev
BACKUP=backup-dev-pre-squash

# THE GROUPING DECISION, IN ONE PLACE.
# SHAs of commits that fold INTO the commit above them. Everything else
# stays a standalone 'pick'. Derived from the revert-or-bisect test:
#   a7edb45 fixes a bug ef64d20 shipped  -> no independent meaning
#   7fe41b4 is a correction found while testing alongside 06998b3
# Valid only against the CURRENT pre-rebase history - every SHA above the
# merge-base changes the moment the rebase runs, so this is single-use.
SQUASH="a7edb45 7fe41b4"

##################
##################

die()  { printf 'ERROR: %s\n' "$1" >&2; exit 1; }
warn() { printf 'WARN:  %s\n' "$1" >&2; }

# GUARD: the silent-failure one.
#
# git falls back to vi when core.editor is unset - survivable, just unpleasant.
# The dangerous case is "code" WITHOUT --wait: it opens the tab and returns
# instantly, so git reads back an untouched todo file, replays all five commits
# unchanged, and reports success. Nothing squashed, no error, no warning. You
# only notice when 'git log' still shows five commits.
check_editor() {
  ed=$(git config --get core.editor || true)
  [ -n "$ed" ] || die "core.editor unset - you would land in vi. fix: git config --global core.editor \"code --wait\""
  case "$ed" in
    *--wait*|*-w*) ;;
    *code*|*subl*|*mate*) die "core.editor is '$ed' - missing --wait. it returns instantly and silently squashes NOTHING." ;;
  esac
}

# GUARD: generated live from git, never hardcoded.
#
# A pasted crib sheet goes stale the moment you add a commit - and a stale one
# does not error, it silently DROPS the commit it forgot to mention. Deriving
# the list from 'git log' makes that impossible rather than merely guarded.
# --reverse because the todo file runs oldest-first, the opposite of git log.
# '%h %s' is exactly the line format git itself writes.
todo_list() {
  git log --reverse --format='%h %s' $BASE..$BRANCH | while read -r sha msg; do
    case " $SQUASH " in
      *" $sha "*) printf 'squash %s %s\n' "$sha" "$msg" ;;
      *)          printf 'pick   %s %s\n' "$sha" "$msg" ;;
    esac
  done
}

# every subcommand below assumes these hold - checked once, up front,
# before anything touches history. cheapest checks first.
preflight() {
  [ "$(git branch --show-current)" = "$BRANCH" ] || die "not on $BRANCH (on $(git branch --show-current))"
  git diff --quiet && git diff --cached --quiet || die "working tree dirty - commit or stash first"
}

usage() {
  echo "Usage: sh git_squash.sh [status|backup|rebase|verify|push|undo|cleanup]"
  echo "  status  - 1. show what will be rewritten, and the exact todo list to paste, 
  for math choosing start and end bounds, then draft rebase in the correct pick sqaush format"
  echo "  backup  - 2. create $BACKUP as a full undo point"
  echo "  rebase  - 3. run the interactive rebase , copy and paste that pick squash format in the correct area, then commit them (opens your editor twice)"
  echo "  verify  - 4. confirm 3 commits and that NO file content changed"
  echo "  push    - 5. force-with-lease to origin/$BRANCH"
  echo "  undo    - !  restore $BRANCH to the backup, abandoning the rebase"
  echo "  cleanup - 6. delete the backup branch once you are happy"
  exit 1
}

main() {
case "$1" in

  ####################################################################
  # STEP 1 - LOOK BEFORE YOU TOUCH
  #
  # Prints the rewritable range and the todo list you will paste in
  # step 3. The SHAs are read live, so this stays honest even if you
  # add another commit before running the rebase.
  #
  # Grouping rationale - a commit should be the smallest thing you
  # would ever want to revert or bisect to on its own:
  #
  #   ef64d20 + a7edb45  -> both are "fix the setup scripts". a7edb45
  #                         fixes a bug ef64d20 shipped, so it has no
  #                         independent meaning.
  #   06998b3 + 7fe41b4  -> both are corrections found while testing
  #                         against the live site.
  #   3a3d276            -> stays alone. it is the "E2E passed" marker,
  #                         the one commit you might genuinely want to
  #                         bisect to later.
  ####################################################################
  status)
    preflight
    echo "base (floor, do NOT rewrite at or below): $(git merge-base $BASE $BRANCH)"
    echo
    echo "rewritable range ($BASE..$BRANCH):"
    git log --oneline $BASE..$BRANCH
    echo
    echo "ahead/behind vs origin/$BRANCH: $(git rev-list --left-right --count origin/$BRANCH...$BRANCH)"
    echo
    echo "the todo file git will hand you, already marked up (OLDEST AT TOP - reverse of git log):"
    echo "-------------------------------------------------------------"
    todo_list
    echo "-------------------------------------------------------------"
    echo
    echo "do NOT paste this. the file git opens is generated from your real history,"
    echo "so it is always complete and current - a crib sheet never is. just change"
    echo "the word 'pick' to 'squash' on the two lines marked above."
    echo
    echo "squash, NOT fixup - fixup discards the second message, and you want both kept."
    ;;

  ####################################################################
  # STEP 2 - SAFETY NET
  #
  # A branch is just a pointer, so this costs nothing and is a
  # complete undo: it pins the pre-rebase commit so it cannot be
  # garbage-collected. reflog would also hold it, but reflog expires
  # and a named branch does not.
  ####################################################################
  backup)
    preflight
    git rev-parse --verify --quiet $BACKUP >/dev/null && die "$BACKUP already exists - run cleanup or pick another name"
    git branch $BACKUP
    echo "backup created at $(git rev-parse --short $BACKUP)"
    echo "undo at any time with: sh git_squash.sh undo"
    ;;

  ####################################################################
  # STEP 3 - THE REBASE
  #
  # Opens your editor with the todo list, then opens it again once
  # per squash group so you can edit the combined message.
  #
  # The '#' lines in the message editor are stripped automatically -
  # save as-is and you get both original messages with a blank line
  # between them. Delete the blank line if you want them adjacent,
  # but note git treats line 1 as the subject and 'git log --oneline'
  # shows only that line.
  #
  # If your editor is vim and you would rather it were not:
  #   git config --global core.editor "code --wait"
  #
  # Stuck mid-rebase? 'git rebase --abort' returns you to the exact
  # starting state. That is always safe.
  ####################################################################
  rebase)
    preflight
    check_editor
    git rev-parse --verify --quiet $BACKUP >/dev/null || warn "no backup branch - consider running 'backup' first"
    # -c applies these to THIS invocation only - no global config is mutated.
    # missingCommitsCheck=error: refuse if the edited todo drops a commit.
    # git's default is 'ignore', which deletes the commit and its changes
    # silently. Only matters if you paste over the file rather than editing
    # two words - but that is exactly when you would never notice.
    git -c rebase.missingCommitsCheck=error rebase -i $BASE
    ;;

  ####################################################################
  # STEP 4 - VERIFY, AND THE CHECK THAT ACTUALLY MATTERS
  #
  # A squash reshapes HISTORY. It must not change FILES. So the diff
  # between the backup and the rewritten branch has to be empty - if
  # it is not, the rebase dropped or mangled something and you should
  # undo rather than investigate on a live branch.
  #
  # This is the one check worth having; commit count is cosmetic.
  ####################################################################
  verify)
    git rev-parse --verify --quiet $BACKUP >/dev/null || die "no $BACKUP branch to compare against"
    echo "commits on $BRANCH since $BASE (expect 3):"
    git log --oneline $BASE..$BRANCH
    echo
    if git diff --quiet $BACKUP $BRANCH; then
      echo "OK - file contents identical to pre-rebase. history rewritten, content untouched."
    else
      git diff --stat $BACKUP $BRANCH
      die "content CHANGED - the rebase lost or altered something. run 'undo'."
    fi
    ;;

  ####################################################################
  # STEP 5 - PUBLISH
  #
  # origin/dev currently points at 3a3d276, which will no longer
  # exist after the rebase, so a normal push is refused. That refusal
  # is git protecting you, not an obstacle.
  #
  # --force-with-lease, never plain --force: it aborts if origin/dev
  # moved somewhere you have not fetched, instead of destroying those
  # commits. Solo private repo makes the risk near zero today - the
  # habit is free and one day it will not be.
  ####################################################################
  push)
    preflight
    git push --force-with-lease origin $BRANCH
    ;;

  ####################################################################
  # UNDO - use at any point after step 2
  #
  # --hard discards the working tree, which is safe here only because
  # preflight refuses to run on a dirty tree. If you are mid-rebase,
  # use 'git rebase --abort' instead; this is for after it finished.
  ####################################################################
  undo)
    git rev-parse --verify --quiet $BACKUP >/dev/null || die "no $BACKUP branch - nothing to restore"
    git reset --hard $BACKUP
    echo "restored $BRANCH to $(git rev-parse --short HEAD)"
    ;;

  ####################################################################
  # STEP 6 - CLEANUP
  #
  # Only after verify passed AND push succeeded. Deleting the backup
  # is the point of no easy return, so it is a separate deliberate
  # step rather than something push does for you.
  ####################################################################
  cleanup)
    git rev-parse --verify --quiet $BACKUP >/dev/null || die "no $BACKUP branch to delete"
    git branch -D $BACKUP
    echo "deleted $BACKUP - reflog still holds the old SHAs for a few weeks if you need them"
    ;;

  *)
    usage
    ;;

esac
}

main "$@"
