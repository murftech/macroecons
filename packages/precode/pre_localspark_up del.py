from pyspark.sql import SparkSession
from pyspark import SparkConf

spark_conf = SparkConf()
spark_conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark_conf.set("spark.sql.session.timeZone","UTC")

spark = SparkSession.builder.appName('localspark').config(conf=spark_conf).getOrCreate()


#######################################

from pyspark.sql import *
from pyspark.sql.functions import *

