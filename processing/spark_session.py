import os

from pyspark.sql import SparkSession

from config import settings

# hadoop-aws must match the Hadoop version bundled with PySpark 3.5.x (3.3.4)
S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def get_spark(app_name="cfpb"):
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.jars.packages", S3A_PACKAGES)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
