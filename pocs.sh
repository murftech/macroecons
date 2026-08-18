
#### how to stage changes into a new feature branch
# lets say i edited on dev etc, or another feature branch


# 1. Create feat/databricks branch from main
git checkout dev
git checkout -b feat/databricks

# 5. Update .gitignore to allowlist the module
# Add the folders into the gitignore.
# Say there are three new folders, then add all of them

# 2. Stage the newly added folders and also anything else that was changed from tracked folders, but you dont want them to be in feat branch
git add .

# 4. Verify what's staged (should be 12 source files, no .databricks/)
git status --porcelain | grep '^A ' | sort

# 7. Commit everything
git commit -m "started new feature dev"

# 8. push to remote.
git push -u origin feat/databricks

# Option 1: Delete untracked files from the previouis feat branch
rm -rf modules/pipe_hdb_cli modules/pipe_hdb_databricks modules/pipe_hdb_minimal



### how to rebase a feat branch, in case dev needed some updates.

git checkout feat/interactive-charts
git rebase dev
