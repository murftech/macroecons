
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

git push --force-with-lease origin feat/interactive-charts



## UV Locking

# in the root folder to be deployed:
# create pyproject.toml
# run uv lock to answer the combinations with uv.lock file.
# us the valid uv.lock to generate a requirements.txt file. Rather than handwritten.
uv export --frozen --no-hashes -o src/requirements.txt
# @ it has to go into src/ for databricks
# The rule of thumb: hashes when you own the entire environment, no hashes when you're layering onto someone else's. Databricks serverless is firmly the second.

cat src/requirements.txt

# pyproject.toml   /  uv.lock          what I want  /  what I got
# databricks.yml   /  resources.json   what I want  /  what got created
# # Declaration and recorded outcome, kept separate. 
# requirements.txt alone is the version where you only keep the outcome and lose the intent/



# How to pull changes into a parent branch. after editing it on a branch.
git stash -u
git checkout dev
git stash pop 
# (Will pop everything that is not already there, will not pop files already named)
# the stash will still be there even if popped.
# find out which files will be replaced, check they are safe and intended. 
# then do:

# rm modules/pipe_hdb/requirements.txt 
# rm pocs.sh 
git stash pop