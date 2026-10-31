from pyspark.sql import functions as F

from spark_session import get_spark

BRONZE_PATH = "s3a://datalake/bronze/taxi_trips/"
SILVER_PATH = "s3a://datalake/silver/taxi_trips/"

# Limites choisies après analyse des données : au-delà, les valeurs sont des erreurs
# (ex. montants à 9999.75 $, compteurs restés allumés pendant 24 h)
MAX_TRIP_SECONDS = 6 * 3600
MAX_TRIP_MILES = 100
MAX_AMOUNT = 500

# Si on rejette plus de 10 % des lignes, il y a sûrement un problème dans les données
MAX_REJECTED_RATE = 0.10


def cast_columns(df):
    """Garde les colonnes utiles et leur donne le bon type (le CSV ne contient que du texte)."""
    return df.select(
        F.col("trip_id"),
        F.col("taxi_id"),
        F.to_timestamp("trip_start_timestamp").alias("trip_start_timestamp"),
        F.to_timestamp("trip_end_timestamp").alias("trip_end_timestamp"),
        F.col("trip_seconds").cast("int"),
        F.col("trip_miles").cast("double"),
        F.col("pickup_community_area").cast("int"),
        F.col("dropoff_community_area").cast("int"),
        F.col("fare").cast("double"),
        F.col("tips").cast("double"),
        F.col("tolls").cast("double"),
        F.col("extras").cast("double"),
        F.col("trip_total").cast("double"),
        F.col("payment_type"),
        F.col("company"),
        F.col("year"),
        F.col("month"),
    )


def clean_trips(df):
    """Supprime les doublons et les lignes incohérentes."""
    # Une comparaison avec une valeur vide n'est jamais vraie : ces lignes sont aussi rejetées.
    # On garde les trajets à 0 mile et sans zone : leur durée et leur prix restent exploitables.
    return df.dropDuplicates(["trip_id"]).filter(
        F.col("trip_id").isNotNull()
        & (F.col("trip_end_timestamp") >= F.col("trip_start_timestamp"))
        & F.col("trip_seconds").between(1, MAX_TRIP_SECONDS)
        & F.col("trip_miles").between(0, MAX_TRIP_MILES)
        & F.col("fare").between(0, MAX_AMOUNT)
        & (F.col("trip_total") > 0)
        & (F.col("trip_total") <= MAX_AMOUNT)
    )


def main():
    spark = get_spark("bronze_to_silver")

    bronze = spark.read.option("header", True).csv(BRONZE_PATH)
    silver = clean_trips(cast_columns(bronze))
    silver.repartition("year", "month").write.mode("overwrite").partitionBy("year", "month").parquet(SILVER_PATH)

    # Contrôle qualité : on compte ce qui a vraiment été écrit
    bronze_rows = bronze.count()
    silver_rows = spark.read.parquet(SILVER_PATH).count()
    rejected_rate = (bronze_rows - silver_rows) / bronze_rows
    print(f"Bronze : {bronze_rows} lignes")
    print(f"Silver : {silver_rows} lignes")
    print(f"Rejetées : {bronze_rows - silver_rows} ({rejected_rate:.2%})")

    if silver_rows == 0:
        raise ValueError("La table silver est vide")
    if rejected_rate > MAX_REJECTED_RATE:
        raise ValueError(f"Trop de lignes rejetées : {rejected_rate:.2%}")

    spark.stop()


if __name__ == "__main__":
    main()
