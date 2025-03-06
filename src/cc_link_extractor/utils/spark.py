from pyspark.sql import SparkSession


def initialize_spark() -> SparkSession:
    return SparkSession.builder.appName(
        "CommonCrawlLinkExtractor"
    ) .config(
        "spark.jars", "postgresql-42.5.1.jar"
    ).getOrCreate()
