from pyspark.sql import SparkSession


def get_spark_session(env):
    if env == "LOCAL":
        spark = SparkSession.builder\
                .config('spark.driver.extraJavaOptions',
                        '-Dlog4j.configuration=file:log4j.properties') \
                .config('spark.executor.extraJavaOptions',
                        '-Dlog4j.configuration=file:log4j.properties') \
                .master('local[3]')\
                .enableHiveSupport()\
                .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
        return spark
    else:
        return SparkSession.builder.enableHiveSupport().getOrCreate()


