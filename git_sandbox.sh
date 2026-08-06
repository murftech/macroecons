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

# if i accideentally commit and push with rushed names.
git commit --amend -m "release" 


# git squash how to i know the content came from where if there is no link anymore?
# future
it might mean every single commit is in main,
And to version release it, we need to name the commits as RELEASE...
Or we need to branch from one of the commits and say release 



# how to sync an not updated workspace

# just 
git checkout 
git pull la

# under the hood
git fetch origin
git checkout main && git merge --ff-only origin/main
git checkout dev  && git merge --ff-only origin/dev
git checkout main


git pull is git fetch + merge, so staleness isn't a risk — it always fetches first.
# wihtout checkout?

### Stash work

git stash list                      
git stash show stash@{0}            

git stash drop

