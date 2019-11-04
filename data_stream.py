import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sqlf
from dateutil.parser import parse as parse_date


# TODO Create a schema for incoming resources
# schema
schema = StructType([
    StructField("crime_id", IntegerType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", TimestampType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", IntegerType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

# TODO create a spark udf to convert time to YYYYmmDDhh format
@psf.udf(StringType())
def udf_convert_time(timestamp):
    date = parse_date(timestamp)
    return str(date.strftime('%y%m%d%H'))


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    # df = spark ...
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9020") \
        .option("subscribe", "service-calls") \
        .option("startingOffserts", "earliest") \
        .option("maxRatePerPartition", 100) \
        .option("maxOffsetPerTrigger", 10) \
        .load()
    
    df.printSchema()

    kafka_df = df.selectExpr("CAST (value AS STRING)")

    service_data_table = kafka_df\
        .select(psf.from_json(sqlf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")

    distinct_datatable = service_data_table\
        .select(sqlf.col('crime_id'),
                sqlf.col('original_crime_type_name'),
                sqlf.to_timestamp(sqlf.col('call_date_time')).alias('call_datetime'),
                sqlf.col('address'),
                sqlf.col('disposition'))

    
    c_df = distinct_datatable\
        .withWatermark("original_crime_type_name", "60 minutes")\
        .groupBy(sqlf.window(distinct_datatable.call_datetime,
                            "10 minutes", 
                            "10 minutes"),
                 distinct_datatable.original_crime_type_name)\
                 .count()

    query = c_df.writeStream.outputMode('Complete').format('console').start()

    # TODO attach a ProgressReporter
    query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    spark = SparkSession.builder \
        .master("local") \
        .appName("SF Crime Statitics") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()

