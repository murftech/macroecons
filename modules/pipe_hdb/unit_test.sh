make -C modules/pipe_hdb land
make -C modules/pipe_hdb land ENV=production
make -C modules/pipe_hdb import
make -C modules/pipe_hdb import ENV=production
make -C modules/pipe_hdb import-full ENV=production

make -C modules/pipe_hdb stage

modules/pipe_hdb/deploy_databricks/databricks_deploy.sh update deploy

modules/pipe_hdb/deploy_databricks/databricks_deploy.sh backfill deploy
