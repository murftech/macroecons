make -C modules/pipe_hdb/deploy_local land
make -C modules/pipe_hdb/deploy_local land ENV=production
make -C modules/pipe_hdb/deploy_local import
make -C modules/pipe_hdb/deploy_local import ENV=production
make -C modules/pipe_hdb/deploy_local import-full ENV=production

make -C modules/pipe_hdb/deploy_local stage

modules/pipe_hdb/deploy_databricks/databricks_deploy.sh update deploy

modules/pipe_hdb/deploy_databricks/databricks_deploy.sh backfill deploy
