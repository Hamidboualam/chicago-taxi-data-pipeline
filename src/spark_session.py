import os

from pyspark.sql import SparkSession


def get_spark(app_name):
    """Crée une session Spark locale connectée au stockage S3 (RustFS)."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
