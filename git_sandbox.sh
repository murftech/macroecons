#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done


git log --oneline
git log --oneline --first-parent

git merge --no-ff dev -m "release to parent"


git log --oneline d41d0d5^1..d41d0d5^2
# what is this?

# work on dev all week, 100 commits, whatever
git checkout main
git merge --no-ff dev -m "Release 2" # this is the key
git push

git log --merges main

# always set this
git config --global merge.ff false
git config --global pull.ff only



# git squash how to i know the content came from where if there is no link anymore?
# future
it might mean every single commit is in main,
And to version release it, we need to name the commits as RELEASE...
Or we need to branch from one of the commits and say release 
