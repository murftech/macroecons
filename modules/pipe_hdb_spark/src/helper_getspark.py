from pyspark.sql import SparkSession

def get_spark(appname = 'localspark'):

    spark = (
        SparkSession.builder
        .appName(appname)
        .config('spark.sql.session.timeZone', 'UTC') # to strip timestamps of Timezone when mutating
        .config('spark.sql.sources.partitionOverwriteMode', 'dynamic') # for replace partitioin
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true') # Accelerates Pandas/PyArrow conversions (eg: write_csv, plotly)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR') # to remove that partition annoying errors, if possible, sub: lets AL test this

    return spark

# sub: i had the issue in write_parititon that the spark was not in the real enriinment. will that happen again now?
