if (!exists('sc')) {
  require(sparklyr)
  sc <<- sparklyr::spark_connect(master = "local")}


if (env=='prod') {hive <- '/Users/murftech/Root/hive'}
if (env=='qa') {hive <- '/Users/murftech/Root/hive_qa'}
sprintf('hive root anchored: %s', hive) %>% print()