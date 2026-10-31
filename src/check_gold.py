from pyspark.sql import functions as F

from spark_session import get_spark

SILVER_PATH = "s3a://datalake/silver/taxi_trips/"
GOLD_PATH = "s3a://datalake/gold"
GOLD_TABLES = ["revenue_per_day", "avg_trip_duration_per_day", "top_pickup_zones"]


def main():
    spark = get_spark("check_gold")

    # Chaque table gold doit contenir des données
    for name in GOLD_TABLES:
        rows = spark.read.parquet(f"{GOLD_PATH}/{name}/").count()
        print(f"{name} : {rows} lignes")
        if rows == 0:
            raise ValueError(f"La table gold {name} est vide")

    # Les trajets comptés en gold doivent être les mêmes qu'en silver
    silver_trips = spark.read.parquet(SILVER_PATH).count()
    revenue = spark.read.parquet(f"{GOLD_PATH}/revenue_per_day/")
    gold_trips = revenue.agg(F.sum("trip_count")).first()[0]
    print(f"Trajets silver : {silver_trips}, trajets gold : {gold_trips}")
    if gold_trips != silver_trips:
        raise ValueError("Le nombre de trajets en gold ne correspond pas au silver")

    print("Contrôle gold OK")
    spark.stop()


if __name__ == "__main__":
    main()
