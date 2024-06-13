from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from pyspark.sql.functions import *

import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DockerSparkExample") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

schema = StructType([
    StructField('student_code', IntegerType(), True),
    StructField('name', StringType(), True)
])

activity_schema = StructType([
    StructField('student_code', IntegerType(), True),
    StructField('activity', StringType(), True),
    StructField('numberOfFile', IntegerType(), True),
    StructField('timestamp', StringType(), True)
])

student_df=spark.read.csv("hdfs://namenode:8020/danh_sach_sv_de.csv", schema=schema)

activity = spark.read.parquet("hdfs://namenode:8020/raw_zone/fact/activity")

aggregated_df = activity.groupBy("student_code", "activity", "timestamp") \
                .agg(sum("numberOfFile").alias("totalFile"))
output_df = aggregated_df.join(student_df, "student_code") \
    .select("timestamp", "student_code", "name", "activity", "totalFile") \
    .orderBy("student_code", "activity")

output_hdfs_path = "hdfs://namenode:8020/output"

output_df.write \
    .mode("overwrite") \
    .option("header", "false") \
    .csv(output_hdfs_path)

spark.stop()