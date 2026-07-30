# import time
# 
# # def tic():
# #     global timestart 
# #     timestart= time.time()
# 
# def is_empty(df):
#     return len(df.head(1))==0

is_empty <- function(df) {
  return(nrow(df) == 0)
}

# 
# 
# # def toc(message='elapsed time'):
# #     print(message)
# #     now = time.time()
# #     print(now - timestart)
# 
# # def create_variable(name):
# #     globals()[name] = 2
# # # Example usage:
# # name = "my_variable"
# # create_variable(name)
# # print(my_variable)  # Access the dynamically created variable
# def noexist(varname):
#     if varname not in globals():
#         return True
#     if varname in globals():
#         return False
# 
# def exist(varname):
#     if varname not in globals():
#         return False
#     if varname in globals():
#         return True
#     
# def tic(name='mastertic'):
#     globals()[name] = time.time()
#     global timestart 
#     if name == 'mastertic':
#         print('mastertic timer has been started')
# 
# def toc(name='mastertic'):
#     now = time.time()
#     print(str(name) + ' took (seconds):')
#     print(now - globals()[name])
# 
# 
# def listvalues(df, colnames, n=100):
#     uniquevals = df.select(colnames).distinct().orderBy(colnames)
#     uniquevals.show(n, truncate=False)
#     return uniquevals
#     
# def dfcol_to_list(df, colname):
#     column_list = df.select(colname).rdd.flatMap(lambda x: x).collect()
#     return column_list
# 

#### Above functions untested conversion via claude cowork

noexist <- function(varname) {
  !exists(varname, envir = .GlobalEnv)
}

exist <- function(varname) {
  exists(varname, envir = .GlobalEnv)
}

tic <- function(name = "mastertic") {
  assign(name, Sys.time(), envir = .GlobalEnv)
  if (name == "mastertic") message("mastertic timer has been started")
}

toc <- function(name = "mastertic") {
  elapsed <- as.numeric(Sys.time() - get(name, envir = .GlobalEnv), units = "secs")
  message(name, " took (seconds):\n", elapsed)
}

listvalues <- function(df, colnames, n = 100) {
  uniquevals <- df |>
    select(all_of(colnames)) |>
    distinct() |>
    arrange(across(all_of(colnames)))
  print(uniquevals, n = n)
  invisible(uniquevals)
}

dfcol_to_list <- function(df, colname) {
  df |> pull({{ colname }})
}

####
matchrowcount <- function(df1, df2) {
  
  rowcount1 <- nrow(df1)
  rowcount2 <- nrow(df2)
  
  if (rowcount1 == rowcount2) {
    print('stayed same. pass.')
    mrc <<- 0
  }
  if (rowcount1 != rowcount2) {
    print('rows different! pcnt difference shown')
    pcnt_delta <- rowcount2 / rowcount1
    print(pcnt_delta)
    mrc <<- 1
    print('global indicator mrc==1 available for raising exception')
  }
}

# 
# 
# def dfratio(df_before, df_after):
#     tic('frun')
#     global ratio_delta
#     before_count = df_before.count()
#     after_count = df_after.count()
#     ratio_delta = after_count/before_count
#     print(f'before count: {before_count}')
#     print(f'after count: {after_count}')
#     print(f'ratiodelta: {(ratio_delta, 3)}')
#     print('ratio_delta returned as global variable for exception raising')
#     toc('frun')
#     
# def in_a_b(table_a, table_b, variable):
#     tic('frun')
#     global intset
#     distint_a = table_a.select(variable).distinct().withColumn('in_a', lit(1))
#     distint_b = table_b.select(variable).distinct().withColumn('in_b', lit(1))
#     intset = distint_a.join(distint_b, on = variable, how='outer')
#     intset.cache()
#     intset.groupBy('in_a', 'in_b').count().orderBy('in_a', 'in_b').show()
#     toc('frun')
#     print('results table avail in global env by calling intset')
#     
# 

in_a_b <- function(table_a, table_b, variable) {
  start_time <- proc.time()
  
  distinct_a <- table_a %>% select(all_of(variable)) %>% distinct() %>% mutate(in_a = 1)
  distinct_b <- table_b %>% select(all_of(variable)) %>% distinct() %>% mutate(in_b = 1)
  
  intset <<- full_join(distinct_a, distinct_b, by = variable)
  
  intset %>%
    group_by(in_a, in_b) %>%
    summarise(count = n(), .groups = "drop") %>%
    arrange(in_a, in_b) %>%
    print()
  
  elapsed <- proc.time() - start_time
  cat(sprintf("frun: %.3f sec elapsed\n", elapsed["elapsed"]))
  cat("results table avail in global env by calling intset\n")
}

sortcount <- function(df, coltuple) {
  factor_table <- df %>% 
    group_by(across(all_of(coltuple))) %>% 
    summarise(count = n()) %>% 
    arrange(desc(count))
  
  # print(factor_table)
  
  table_total <- sum(factor_table$count)
  # cat(table_total, "\n")
  
  factor_table_pcnt <- factor_table %>% 
    mutate(pcnt = count / table_total) %>%
    arrange(desc(count))

  
  return(factor_table_pcnt)
}

# 
# def sortcount(df, coltuple, truncate=True):
#     tic('sortcount_run')
#     factor_table = df.groupBy(coltuple).count().sort(desc('count'))
# #     totals = df.count().withColumnRenamed('count', 'total')
# #     factor_table_pcnt = factor_table.crossJoin(totals).withColumn('pcnt', col('count')/df.count())
#     factor_table_pcnt = factor_table.withColumn('pcnt', col('count')/df.count())
#     factor_table_pcnt.cache()
#     factor_table_pcnt.show(40, truncate=truncate)
#     toc('sortcount_run')
#     return factor_table_pcnt
# 
# 
# def dim(self):
#     tic('frun')
#     print(self.count(), len(self.columns))
#     toc('frun')
# 
# 
# 
# def datescope_dataset(df, datecol, breakdown = 'daily', show=False):
# 
#     if breakdown == 'daily':
#         list_dates = df.select(datecol).distinct().orderBy(desc(datecol))
#         if show:
#             list_dates.cache()
#             list_dates.show(30)
#         return list_dates
#     if breakdown == 'monthly':
#         df = (df
#              .withColumn('year', year(col(datecol)))
#              .withColumn('month', month(col(datecol)))
#              )
#         list_months = df.select('year', 'month').distinct().orderBy(desc('year'), desc('month'))
#         if show:
#             list_months.cache()
#             list_months.show()
#         return list_months
#     
#     
# 
# def monthscopecheck_old(table, datecol, grouping=[]):
#     table = table.withColumn('year', year(datecol)).withColumn('month', month(datecol))
#     grouptile = grouping + ['year', 'month']
# #     year(datecol), month(datecol)
#     scopemonth_return = table.groupBy(grouptile).count().orderBy(grouptile)
#     
#     return scopemonth_return
#     
#     
# 
# def monthscopecheck2(table, datecol, grouping=[]):
#     dates_sel = table.select(datecol).distinct()
#     months_sel = dates_sel.withColumn('year', year(datecol)).withColumn('month', month(datecol))
#     grouptile = grouping + ['year', 'month']
# #     year(datecol), month(datecol)
#     scopemonth_return = months_sel.groupBy(grouptile).count().orderBy(grouptile)
#     
#     return scopemonth_return
#     
#     
# 
# def monthscopecheck(table, datecol, grouping=[], inner=0):
#     tic('monthscopecheck frun')
#     grouptile = grouping + ['year', 'month']
# #     dates_sel = table.select(grouping + [datecol]).distinct()
#     dates_sel = table.dropDuplicates(grouping + [datecol]).select(grouping + [datecol])
#     months_sel = make_yearmonth(dates_sel, datecol)
#     
#     if inner == 1:
#         innerview = months_sel.groupBy(grouptile).agg(countDistinct(datecol)).orderBy(grouptile)
#         innerview.show(10000)
# 
#     if inner == 0:
#         listvalues(months_sel, grouptile, n=10000)
#     toc('monthscopecheck frun')
# 
#     
# 
# # def monthlydates(table, datecol, grouping=[]):
# #     tic('frun')
# #     grouptile = grouping + ['year', 'month']
# #     dates_sel = table.select(grouping + [datecol]).distinct()
# #     months_sel = make_yearmonth(dates_sel, datecol)
# #     months_sel.groupBy(grouptile).agg(countDistinct('datecol'))
# #     toc('frun')
#     
#     
# 
# def summarydate(table, datecol, choice='all', grouping=[], ):
#     global dates_summary
#     tic('frun')
#     
#     if choice=='min':
# 
#         dates_summary = (table
#                      .groupBy(grouping)
#                      .agg(
#                          min(datecol).alias('maxdate'),
#                      ).orderBy(grouping)
#                         )
# 
#     if choice=='max':
#         dates_summary = (table
#                      .groupBy(grouping)
#                      .agg(
#                          max(datecol).alias('maxdate')
#                      ).orderBy(grouping)
#                         )
#     
#     if choice=='all':
#         dates_summary = (table
#                      .groupBy(grouping)
#                      .agg(
#                          max(datecol).alias('maxdate'),
# #                          mean(datecol).alias('meandate'),
#                          min(datecol).alias('mindate'),
#                      ).orderBy(grouping)
#                         )    
#     dates_summary.cache()
#     dates_summary.show(1000, truncate=False)
#     toc('frun')
#     
#     print('dates_summary in global for reprinting if need.')
# 
# 
#     
# 
# def unicity_old(dataset, coltuple):
#     unitable = dataset.groupby(coltuple).count().sort(desc('count'))
#     multiplicity = unitable.filter(col('count') > 1)
#     # unitable.show()
#     score = multiplicity.count()
# 
#     if (score == 0):
#         print("good!: multiplicity passed!:")
# 
#     if (score != 0):
#         print("warning: multiplicity violated! Showing rows:")
#         multiplicity.show()   
#         

unicity <- function(dataset, coltuple, showpcnt=TRUE) {
  print('victims will be returned in global as uvictims')
  
  unitable <- dataset %>%
    group_by(across(all_of(coltuple))) %>%
    summarise(nb_rows = n()) %>%
    arrange(desc(nb_rows))
  
  print(unitable)
  multiplicity = unitable %>% filter(nb_rows > 1)
  
  if (is_empty(multiplicity)) 
  {
    print("good!: multiplicity passed!, indicator unipass = 1")
    uvictims <<- NULL
    unipass <<- 1
  }
  else
  {
    print("warning: multiplicity violated! Show keys. inidicator unipass = 0")
    print(multiplicity)
    uvictims <<- dataset %>% semi_join(multiplicity, by=coltuple)
    unipass <<- 0
    
    
    print('victims are returned in global as uvictims. can cancel the show pcnt if you want')
    
    if (showpcnt)
    {
      print('multiplicity pcnt')
      print(sortcount(unitable, 'nb_rows'))
    }
  }
  print('unicity ended')
}




# def prikey_forcecheck(df, prikeycols):
#     tic('prikey_forcecheck run')
#     print('prikey checks no matter what, and if prikey dont match just take a simple, drop first row ignore.')
#     print('unicheck, if passes, function ends')
#     unicity(df, prikeycols)
#     if uvictims is None:
#         print('returned the exact same df')
#         return df
#     
#     if uvictims is not None:
#         print('prikey broken applying brute dedup')
#         df.dropDubplicates('form_id')
#         
#         df_dedup = window_top_n(df, partition=prikeycols, orderBy=['form_id'], top_n=1)
# #         df_dedup = df.dropDuplicates(prikeycols)
#         print('confirm it is deduped:')
#         unicity(df_dedup, prikeycols)
#         toc('prikey_forcecheck run')
#         return df_dedup
#     
# 
# def cdt(dataframe, grouping_tuple, colname):
#     groupcount = dataframe.groupBy(grouping_tuple).agg(countDistinct(colname).alias('nb_'+ colname))
#     groupcount = groupcount.orderBy(grouping_tuple)
#     groupcount.printSchema()
#     return groupcount
# 
# 
# ########
# #### sanity check wrappers
# #######
# 
# 
# 
# def nullpcnt(df, var, groupcol = []):
#     global nullindicator
# 
#     print('checking nullpcnt for: ' + var)
#     grouptile =  [col(var).isNotNull()] + groupcol
#     pcntnull = sortcount(df, grouptile)
#     
#     if False in dfcol_to_list(pcntnull, pcntnull[0]):
#         nullindicator = 1
#     else:
#         nullindicator = 0
#     print('global variable returned as nullindicator=1 if null values exist')
#     return pcntnull
# 
# 
# def dval_nullpcnt(dataframe, groupcol =[], return_results=0):
#     tic('nullelapsed')
#     dflist = []
# 
#     for var in dataframe.columns:
#         pcntnull = nullpcnt(dataframe, var, groupcol)
# 
# #         pcntnull = sortcount(dataframe, col(var).isNotNull())
# 
#         if return_results==1:
#             first_column_name = pcntnull.columns[0]
#             pcntnull = pcntnull.withColumnRenamed(first_column_name, 'isNotNull')
#             pcntnull = pcntnull.withColumn('variable', lit(var))
#             dflist.append(pcntnull)
#         toc('nullelapsed')
# 
# 
#     if return_results==1:
#         results = stack_dflist(dflist)
#         results = rearrange_to_front(results, ['variable'])
#         results.cache()
#         print('returned total results')
#         results.show()
#         toc('nullelapsed')
#         return results
# 
#     
#               


nullpcnt <- function(df, var, groupcol = character(0)) {
  cat("checking nullpcnt for:", var, "\n")
  
  group_vars <- c("isNotNull", groupcol)
  
  pcnt_null <- df %>%
    mutate(isNotNull = !is.na(.data[[var]])) %>%
    group_by(across(all_of(group_vars))) %>%
    summarise(count = n(), .groups = "drop") %>%
    arrange(desc(count))
  
  if (FALSE %in% pcnt_null$isNotNull) {
    null_indicator <<- 1
  } else {
    null_indicator <<- 0
  }
  
  cat("global variable returned as null_indicator=1 if null values exist\n")
  pcnt_null
}


dval_nullpcnt <- function(df, groupcol = character(0), return_results = FALSE) {
  start_time <- proc.time()
  df_list <- list()
  
  for (var in colnames(df)) {
    pcnt_null <- null_pcnt(df, var, groupcol)
    
    if (return_results) {
      pcnt_null <- pcnt_null %>%
        mutate(variable = var) %>%
        select(variable, everything())
      df_list[[var]] <- pcnt_null
    }
    
    cat("Elapsed:", (proc.time() - start_time)["elapsed"], "s\n")
  }
  
  if (return_results) {
    results <- bind_rows(df_list)
    print(results)
    cat("Total elapsed:", (proc.time() - start_time)["elapsed"], "s\n")
    return(results)
  }
}
