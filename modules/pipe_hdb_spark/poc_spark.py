

# what was there in the past?

NOTE: all taken except types import * which i am still figuring if i should.
# The non highlighted ones are stuff that i used all along.
# Why did i have to load them?


# from pyspark.sql import SparkSession
# del from pyspark.sql.functions import *
# should not do this anymore

# sub are there ever any any any need to do thos stupid engine configs? are is those outdated things now?

from pyspark.sql.types import *
# sub, is it safe to import *
from pyspark.sql.window import Window
# sub: should we seperate a pyspark startup helper and pyspark functions load helper?
# yes i think we should.

def get_spark(app_name="local", master="local[*]", log_level="WARN"):
    # from pyspark.sql import SparkSession
    # del from pyspark import SparkConf
    # del spark_conf = SparkConf()
    # can be not needed at all, extra lines

    # Learn about this option, about the incremental, snapshot, etc, learn all of the options, and what the hell is this really for.
    # learn how is this set up in cloud. In fact this is the most important
    spark_conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    # lp what is this needed for, is it to deal with Signapor timeslatmp or what. Its during processing right?
    # spark_conf.set("spark.sql.session.timeZone","UTC")
    # spark = SparkSession.builder.appName('localspark').config(conf=spark_conf).getOrCreate()

    #######################################

    # from pyspark.sql import *
    # from pyspark.sql.functions import col, lit, when, input_file_name    
    # from pyspark.sql.functions import *

    spark.sparkContext.setLogLevel(log_level)
    # do we even need this, what is this for?

    # Your own message so you always see something
    # print(f"[precode] Spark started: app={app_name}, master={master}, log={log_level}")

    # return spark



from pyspark.sql.types import *
Might be okay to do this?
# Types (pyspark.sql.types as T) have zero collisions. 
# All PySpark data types use CamelCase ending in 
# Type (e.g., StringType, IntegerType, StructType), which never shadow Python built-in 
# lowercase types (str, int, dict).




