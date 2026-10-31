from spark_session import get_spark

PATH = "s3a://datalake/test/spark_check/"

spark = get_spark("check_spark_s3")

# Écrit un petit fichier Parquet dans RustFS
df = spark.createDataFrame([(1, "bronze"), (2, "silver"), (3, "gold")], ["id", "layer"])
df.write.mode("overwrite").parquet(PATH)

# Le relit pour vérifier que les données sont bien là
result = spark.read.parquet(PATH)
result.show()

if result.count() != 3:
    raise ValueError("Le fichier relu ne contient pas 3 lignes")

print("OK : Spark sait écrire et lire dans RustFS")
spark.stop()
