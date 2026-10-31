FROM apache/airflow:2.10.5-python3.11

# Java : nécessaire pour faire tourner Spark
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.6

# Connecteur S3 : permet à Spark de lire et écrire dans RustFS
# (hadoop-aws doit avoir la même version que le Hadoop inclus dans PySpark)
RUN cd /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
