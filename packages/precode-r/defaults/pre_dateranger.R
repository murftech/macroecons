# from pyspark.sql import *
# from pyspark.sql.functions import *
# from pyspark.sql.functions import col
# 

dfilter <- function(df, datecol, startDate, endDate) {
  df %>% filter(.data[[datecol]] >= as.Date(startDate) & .data[[datecol]] <= as.Date(endDate) )
}



# def dfilter(df, datecol, dateStart, dateEnd):
#     return df.filter(col(datecol) <= dateEnd).filter(col(datecol) >= dateStart)
# 
# 
# def firstdate(df, yearcol, monthcol, outputcolname):
#     df_dated = df.withColumn(outputcolname, concat_ws("-",col(yearcol),col(monthcol),lit(1)).cast("date"))
#     return df_dated
# 
#     
# def appendCal(dataframe, datecol, dtb_date_factors):
#     df_match = dataframe.withColumn('date', col(datecol))
#     df =df_match.join(dtb_date_factors, on=['date'])
#     df.printSchema()
#     return df
# # sample
# # dtb_date_factors = spark.read.parquet('murphy/t3/dtb_date_factors.parquet')
# # mtb_gc_form = appendCal(mtb_gc_form, 'gc_started_date', dtb_date_factors)
# 
# 
# from datetime import datetime, timedelta
# 
# def date_add_days(date_str, n):
#     # Convert the date string to a datetime object
#     date_obj = datetime.strptime(date_str, '%Y-%m-%d')
#     
#     # Add n days to the date
#     new_date_obj = date_obj + timedelta(days=n)
#     
#     # Convert the new date object back to a string
#     new_date_str = new_date_obj.strftime('%Y-%m-%d')
#     
#     return new_date_str
# 
# 

make_yearmonth <- function(df, timecol) {
  dateddf <- df %>%
    mutate(year = year(.data[[timecol]]),
           month = month(.data[[timecol]]),
           monthdate = make_date(year, month, 1)) %>%
    mutate(year = as.character(year),
           month = as.character(month) %>% str_pad(2, 'left', '0')
           )
  
  return(dateddf)
}



datedize_y4m2 <- function(digit6month) {
  year = digit6month %>% str_sub(1,4) %>% as.numeric()
  month = digit6month %>% str_sub(5,6) %>% as.numeric()
  day = 1
  return(make_date(year, month, day))
}


csv_date_roulette_parse <- function(df, datecol) {
  
  # 1. Define your "Suspect" formats
  # Note: lubridate uses orders like "ymd", "dmy", "mdy"
  # These are much more flexible than strict format strings
  format_order <- c('dmy', "mdy", 'ymd')
  
  # 2. Attempt parsing
  # parse_date_time handles the "coalesce" logic internally when given a vector of orders
  df_cleaned <- df %>%
    mutate(
      date_cleaned = parse_date_time(!!sym(datecol), orders = format_order) %>% as.Date()
    )
  
  # 3. Check for Nulls (NAs in R)
  original_vals <- df_cleaned %>% pull(!!sym(datecol))
  null_count <- sum(!is.na(original_vals) & is.na(df_cleaned$date_cleaned))
  
  if (null_count > 0) {
    # print(df_cleaned %>% select(all_of(c(datecol, "date_cleaned"))) %>% head())
    print(df_cleaned %>% filter(is.na(date_cleaned)))
    stop(paste("date parsed returned", null_count, "nulls, break!"))
  }
  
  # 4. Check for "Wrong Year" (e.g., 0026 instead of 2026)
  wrong_year_df <- df_cleaned %>% 
    filter(year(date_cleaned) <= 1990)
  
  if (nrow(wrong_year_df) > 0) {
    print(wrong_year_df %>% select(date_cleaned) %>% head())
    stop("year parsed into 00xx formats, break!")
  }
  
  # 5. Cleanup and replace original column
  df_cleaned <- df_cleaned %>%
    mutate(!!sym(datecol) := date_cleaned) %>%
    select(-date_cleaned)
  
  return(df_cleaned)
}



# 
# 
# 
# def maxbusinessdate(adadf): 
#     from datetime import datetime, timedelta
#     import pytz
#     nowtime = datetime.now(tz=pytz.timezone("Asia/Singapore"))
#     nowtime
#     
#     n=1
#     condition=True
#     while condition:
#         minus_n = nowtime - timedelta(days=n)
#         minus_n = minus_n.strftime("%Y-%m-%d")
#         minus_n
#         df_businessdate = adadf.filter(col('businessdate')==minus_n)
#         df_businessdate.cache()
#         # each of this takes 20 seconds though! oh if it is zero it takes 1 seconds only
#         tic()
#         nb_rows = df_businessdate.count()
#         condition = nb_rows==0
#         toc()
# 
#         if condition == True:
#             print("still no data on date: ", minus_n)
#             n=n+1
#         else:
#             print("data found on date: ", minus_n)
#             print("with nb_rows = ", nb_rows)
#             break
#     return(df_businessdate)
