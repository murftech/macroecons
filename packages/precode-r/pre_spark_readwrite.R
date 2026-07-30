

# testers
# hdfs_path = 't1/spendlog'
# env='qa'
# datecol='monthdate'
# startDate = '2023-01-01'
# endDate = '2023-04-01'


viewdata <- function(hdfs_path, datecol=NA, startDate=NA, endDate=NA, env='qa')
{
  runpre('pre_localspark_up.R')
  
  viewloaded <- rpart(hdfs_path, datecol, startDate, endDate, env)
  viewloaded <- viewloaded %>% arrange(.data[[datecol]])
  write_csv(viewloaded, '/Users/murftech/Root/Datarepo/hiveviews/tmp.csv', na="")
  system("open /Users/murftech/Root/Datarepo/hiveviews/tmp.csv")
  
  # sub add option for desc order next time
  # add option to excel open next time, freeze panes, make into table, etc.
}

rpart <- function(hdfs_path, datecol=NA, startDate=NA, endDate=NA, env='qa', limit=NA) {
  
  runpre('pre_localspark_up.R')
  
  fullpath = file.path(hive, hdfs_path)
  keyname = make.names(hdfs_path) %>% str_replace_all('\\.', '_')
  df_connect <- spark_read_parquet(sc, name=keyname, path = fullpath)
  if (!is.na(datecol)) {
    sprintf('applied daterange filter: %s ~ %s', startDate, endDate)
    df_connect <- dfilter(df_connect, datecol, startDate, endDate)
  }
  
  if (!is.na(limit)) {
    df_connect <- head(df_connect, limit)
  }
  print('returning rdataframe')
  r_df <- collect(df_connect)
  
  print('schema of loaded table:')
  glimpse(r_df)
  
  return(r_df)
}

write_partition <- function(dataframe, partition_list, hive_folder_path, env='qa', mode='overwrite') {
  env <<- env
  runpre('pre_localspark_up.R')
  
  # if (rpart(hive_folder_path).columns != mtbgc_churn_schema.columns):
  #     raise Exception ('targetcols list unmatched! do not push! check first')
  if (is_empty(dataframe)) {
    print("EMPTY DATASET RETURNED!, NO POINT PUSHING. FUNCTION SKIPPED. POSSIBLE there's error in one of the pipes.")
    return(0)
  }
  
  print('writing partition started')
  
  fullpath = file.path(hive, hive_folder_path)
  print('files written to path:')
  print(fullpath)
  
  pushspark <- copy_to(sc, dataframe, name = "copy", overwrite=TRUE)
  spark_write_parquet(pushspark, path = fullpath, partition_by = partition_list, mode = mode)

  print('writing partition done')
}


# testers
# dataframe <- period2
# # partition_list <- c('monthdate')
# partition_list <- c('year', 'monthdate')
# hive_folder_path <- 't1/finance_datalake'
# # replace_partition(financelog_all, c('year', 'monthdate'), 't1/finance_datalake')
# # replace_partition(period1, partition_list, 't1/finance_datalake')


contains_subdirectory <- function(directory_path) {
  # Get a list of items (files and directories) in the given directory
  items <- list.files(directory_path, full.names = TRUE)
  
  # Check if any of the items are directories
  any(sapply(items, function(item) file.info(item)$isdir))
}


replace_partition <- function(dataframe, partition_list, hive_folder_path)
{
  runpre('pre_localspark_up.R')
  
  if(!contains_subdirectory(file.path(hive, hive_folder_path))) {
    print('hive empty. write, not replace partition')
    write_partition(dataframe, partition_list, hive_folder_path, mode='overwrite')
    return(0)
  }
  
  print('schema sync enforce 1: intsect col typing same.')
  dummyload <- rpart(hive_folder_path, limit=1)
  print('dummyload done')
  require(janitor)
  if (!compare_df_cols_same(dummyload, dataframe)) {stop('typing violated. PUSH RESTRICTED to protect parquet')}
  else {print('typing passed. peaceful validation')}
  
  print('schema sync enforce 2: push data must have ALL columns of destination schema')
  missing_columns <- setdiff(names(dummyload), names(dataframe))
  missing_columns
  
  if (length(missing_columns)>0) {
    print(missing_columns)
    stop('missing schema columns. Push is restricted!')
  } else {print('intersect passed. peaceful validation')}
  
  
  print('schema sync enforce 3: Push data advisable not to have extra columns')
  extra_columns <- setdiff(names(dataframe), names(dummyload))
  extra_columns
  
  if (length(extra_columns)>0) {
    print(extra_columns)
    print('WARN: partitions pushed with extra columns. pushing is allowed to continue. could cause future complications, that need to manually resolve')
    stop('extra columns, error!')
  } else {print('cols A = cols B. best case.')}
  
  write_partition(dataframe, partition_list, 'tmp', mode='overwrite')
  
  library(fs)
  
  print('start replace partition')
  bau_parquet <- file.path(hive, hive_folder_path)
  # print(bau_parquet)
  tmp_parquet <- file.path(hive, 'tmp')
  # print(tmp_parquet)
  
  # old method
  # new_partitions <- dir(tmp_parquet)[str_detect(dir(tmp_parquet), partition_list)]
  # print('new partitions: ')
  # print(new_partitions)
  
  # new method
  minora_part = partition_list[length(partition_list)]
  new_partitions <- list.dirs(tmp_parquet)
  new_partitions = new_partitions[grep(minora_part, new_partitions)]
  # # # its tough also.
  # later ba.
  
  partition = new_partitions[5]
  for (partition in new_partitions) {
    # # old method
    # bau_fs <- file.path(bau_parquet, partition)
    # "/Users/murftech/Root/hive_qa/t1/finance_datalake/monthdate=2014-05-01"
    # tmp_fs <- file.path(tmp_parquet, partition)
    # "/Users/murftech/Root/hive_qa/tmp/monthdate=2014-05-01"
    # new method
    bau_fs <- str_replace(partition, tmp_parquet, bau_parquet)
    tmp_fs <- partition
    # 
    # Check if the partition exists in the old data
    if (!dir_exists(bau_fs)) {
      
      sprintf('creating new: %s', bau_fs) %>% print()
      dir.create(bau_fs, recursive = T)
      # why sometimes no need to create. no need to create if its at the same level.
      file.rename(tmp_fs, bau_fs)
      
    } else {
      sprintf('replacing: %s', bau_fs) %>% print()
      # print('replacing: ') 
      # print(bau_fs)
      
      file_delete(bau_fs)
      file_move(tmp_fs, bau_fs)
    }
  }
}




