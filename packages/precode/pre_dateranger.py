

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col
import precode.pre_dimchecks as dc
import precode.pre_datashapers as ds



def dfilter(df, datecol, dateStart, dateEnd):
    return df.filter(col(datecol) <= dateEnd).filter(col(datecol) >= dateStart)


def firstdate(df, yearcol, monthcol, outputcolname):
    df_dated = df.withColumn(outputcolname, concat_ws("-",col(yearcol),col(monthcol),lit(1)).cast("date"))
    return df_dated

    
def appendCal(dataframe, datecol, dtb_date_factors):
    df_match = dataframe.withColumn('date', col(datecol))
    df =df_match.join(dtb_date_factors, on=['date'])
    df.printSchema()
    return df
# sample
# dtb_date_factors = spark.read.parquet('murphy/t3/dtb_date_factors.parquet')
# mtb_gc_form = appendCal(mtb_gc_form, 'gc_started_date', dtb_date_factors)


from datetime import datetime, timedelta

def date_add_days(date_str, n):
    # Convert the date string to a datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    # Add n days to the date
    new_date_obj = date_obj + timedelta(days=n)
    
    # Convert the new date object back to a string
    new_date_str = new_date_obj.strftime('%Y-%m-%d')
    
    return new_date_str


def make_yearmonth(df, timecol):
    dateddf = (df
               .withColumn('year', year(timecol)).withColumn('month', month(timecol))
               .withColumn("monthdate", to_date(concat(lit(col("year")), lit("-"), lit(col("month")), lit("-01"))))
              )
    return dateddf


def maxbusinessdate(df):
    lastdate = df.agg(max('businessdate')).collect()[0][0]
    print('the last date picked is: ')
    print(lastdate)
    df = df.filter(col('businessdate')==lastdate)
    return df



# testers
# df=date_event
# datecol = 'date'

def csv_date_roullete_parse(df, datecol):

  # 1. Define your "Suspect" formats
  # Note: Use 'yyyy' for 4-digit years and 'y' or 'yy' for 2-digit.
  format_order = ["yyyy-M-d", 'd/M/yy', 'M/d/yy']
  # , "dd/MM/yyyy"
  # 2. Build a list of to_date expressions
  date_attempts = [to_date(datecol, f) for f in format_order]

  # 3. Use coalesce to pick the first one that doesn't return null
  # We also include the original column cast to Date in case it's already a Date type
  df_cleaned = df.withColumn(
      "date_cleaned", 
      coalesce(col(datecol).cast('date'), *date_attempts)
  )
  dc.showcol(df_cleaned, 'date_cleaned')

  dc.nullpcnt(df_cleaned, 'date_cleaned')

  if dc.nullindicator==1:
      raise Exception('date parsed returned some nulls, break!')

  wrong_year = df_cleaned.filter(year(col('date_cleaned'))<=1990)
  if wrong_year.count() != 0:
      dc.showcol(wrong_year, 'date_cleaned')
      raise Exception('year parsed into 0025 formats, break!')
  
  df_cleaned = (df_cleaned
                .withColumn(datecol, col('date_cleaned'))
                .drop('date_cleaned')
  )

#   df_cleaned = ds.rearrange_to_front(df_cleaned, 'date')


  return df_cleaned


def shift_monthdate(start_month: str, n: int = -1) -> str:
    from dateutil.relativedelta import relativedelta
    from datetime import date

    shifted = date.fromisoformat(start_month) + relativedelta(months=n)
    monthdate_iso = shifted.isoformat()
    return monthdate_iso

# def maxbusinessdate(adadf): 
#     from datetime import datetime, timedelta
#     import pytz
#     nowtime = datetime.now(tz=pytz.timezone("Asia/Singapore"))
#     nowtime
    
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

#         if condition == True:
#             print("still no data on date: ", minus_n)
#             n=n+1
#         else:
#             print("data found on date: ", minus_n)
#             print("with nb_rows = ", nb_rows)
#             break
#     return(df_businessdate)
