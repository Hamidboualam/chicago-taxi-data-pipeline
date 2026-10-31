from pyspark.sql import functions as F

from spark_session import get_spark

SILVER_PATH = "s3a://datalake/silver/taxi_trips/"
GOLD_PATH = "s3a://datalake/gold"
COMMUNITY_AREAS_PATH = "file:///opt/airflow/data/community_areas.csv"
TOP_ZONES = 10


def revenue_per_day(trips):
    """Nombre de trajets et revenu total par jour."""
    return (
        trips.groupBy(F.to_date("trip_start_timestamp").alias("trip_date"))
        .agg(F.count("*").alias("trip_count"), F.round(F.sum("trip_total"), 2).alias("revenue"))
        .orderBy("trip_date")
    )


def avg_trip_duration_per_day(trips):
    """Durée moyenne des trajets par jour, en minutes."""
    return (
        trips.groupBy(F.to_date("trip_start_timestamp").alias("trip_date"))
        .agg(F.round(F.avg("trip_seconds") / 60, 2).alias("avg_duration_minutes"))
        .orderBy("trip_date")
    )


def top_pickup_zones(trips, community_areas):
    """Zones avec le plus de prises en charge. Les trajets sans zone sont exclus par la jointure."""
    return (
        trips.join(community_areas, trips.pickup_community_area == community_areas.community_area)
        .groupBy("community_area", "community_name")
        .agg(F.count("*").alias("trip_count"), F.round(F.sum("trip_total"), 2).alias("revenue"))
        .orderBy(F.desc("trip_count"))
        .limit(TOP_ZONES)
    )


def write_table(df, name):
    # Petites tables : un seul fichier suffit
    df.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_PATH}/{name}/")
    print(f"Table gold {name} écrite")


def main():
    spark = get_spark("silver_to_gold")

    trips = spark.read.parquet(SILVER_PATH)
    community_areas = spark.read.option("header", True).csv(COMMUNITY_AREAS_PATH)
    community_areas = community_areas.withColumn("community_area", F.col("community_area").cast("int"))

    write_table(revenue_per_day(trips), "revenue_per_day")
    write_table(avg_trip_duration_per_day(trips), "avg_trip_duration_per_day")
    write_table(top_pickup_zones(trips, community_areas), "top_pickup_zones")

    spark.stop()


if __name__ == "__main__":
    main()
