# import inspect
# 
# 



# new_prikey_table= log_paynow
# prikeys = 'item_id'
# user_cols = 'business'
# file_url='/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/annotations/rtb/log_paynow_business.csv'

refresh_manual_file <- function(new_prikey_table, prikeys, user_cols, file_url) 
{
  load_file <- read_csv(file_url)
  load_file <- load_file %>% select(all_of(prikeys), all_of(user_cols))
  print(load_file)
  
  print('attaching user_cols')
  load_file_updated <- load_file %>% full_join(new_prikey_table, by=prikeys)
  # do this only in future
  # load_file_updated %>% arrange({{user_cols}})
  
  unicity(load_file_updated, prikeys)
  if (!is.null(uvictims)) {stop('prikey_violated. no push and ruin the user file')}
  
  computed_cols <- setdiff(names(new_prikey_table), c(prikeys, user_cols))
  ready_file <- load_file_updated %>% select(all_of(prikeys), computed_cols ,all_of(user_cols))
  write_csv(ready_file, file_url, na="")
  system(paste("open", shQuote(file_url)))
  
  return(ready_file)
}


#     
# def io_tic():
#     global io_start 
#     io_start= time.time()
# 
# def io_toc(message='io duration'):
#     print(message)
#     io_end = time.time()
#     print(io_end - io_start)
# 
# def write_csv(sparkdf, csvpath):
#     io_tic()
#     cdsw_csvpath = '/home/cdsw/' + csvpath 
#     dstination_folder = os.path.dirname(cdsw_csvpath)
#     
#     if not os.path.exists(dstination_folder):
#         print('directory doesnt exist, will be created.')
#         os.makedirs(dstination_folder)
#     sparkdf.toPandas().to_csv(cdsw_csvpath ,index=False, na_rep='')
#     print('file written to: ' + cdsw_csvpath)
#     io_toc()

# #### parquet writing ####    
# def write_partition(dataframe, partition_list, hive_folder_path):  
# #     alert('writing partition start')
#     # print('validation checks')

# #     if (dataframe.count()) == 0:
# #         raise Exception("empty dataset returned, no point pushing, and definitely there's error in any pipe. Debug!")
#     print('writing partition started')
#     tic('io')
#     dataframe.write.parquet(hive_folder_path, mode='overwrite', partitionBy = partition_list)
#     print('writing partition done')
# #     alert('writing partition end')
#     toc('io')
# 
#     


