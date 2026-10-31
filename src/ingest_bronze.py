import argparse
import os
import time

import boto3
import requests

API_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
BUCKET = "datalake"
PAGE_SIZE = 50000

# Période choisie : premier trimestre 2023, mois par mois (début, fin exclue)
PERIODS = [
    ("2023-01-01", "2023-02-01"),
    ("2023-02-01", "2023-03-01"),
    ("2023-03-01", "2023-04-01"),
]


def fetch_page(start, end, limit, offset):
    """Télécharge une page de trajets au format CSV (3 essais si l'API échoue)."""
    params = {
        "$where": f"trip_start_timestamp >= '{start}T00:00:00' AND trip_start_timestamp < '{end}T00:00:00'",
        # Le tri rend la pagination stable. Trier sur trip_id seul est trop lent sur cette API.
        "$order": "trip_start_timestamp,trip_id",
        "$limit": limit,
        "$offset": offset,
    }
    for attempt in range(1, 4):
        try:
            response = requests.get(API_URL, params=params, timeout=300)
            response.raise_for_status()
            return response.text
        except requests.RequestException as error:
            print(f"Essai {attempt}/3 en échec : {error}")
            if attempt == 3:
                raise
            time.sleep(10)


def delete_old_files(s3, prefix):
    """Supprime les anciens fichiers du mois : relancer le script ne crée pas de doublons."""
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
    for obj in objects:
        s3.delete_object(Bucket=BUCKET, Key=obj["Key"])


def ingest_month(s3, start, end, max_rows):
    """Copie un mois de données dans bronze, sans les modifier, un fichier CSV par page."""
    year, month = start[:4], start[5:7]
    prefix = f"bronze/taxi_trips/year={year}/month={month}/"
    delete_old_files(s3, prefix)

    total_rows = 0
    part = 0
    while True:
        limit = PAGE_SIZE
        if max_rows is not None:
            limit = min(PAGE_SIZE, max_rows - total_rows)

        page = fetch_page(start, end, limit, total_rows)
        rows = len(page.splitlines()) - 1  # sans la ligne d'en-tête
        if rows == 0:
            break

        key = f"{prefix}part-{part:05d}.csv"
        s3.put_object(Bucket=BUCKET, Key=key, Body=page.encode("utf-8"))
        total_rows += rows
        part += 1
        print(f"{key} : {rows} lignes (total du mois : {total_rows})")

        if rows < limit or total_rows == max_rows:
            break

    print(f"Mois {year}-{month} terminé : {total_rows} lignes")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-rows", type=int, help="limite de lignes par mois, pour tester")
    args = parser.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    for start, end in PERIODS:
        ingest_month(s3, start, end, args.max_rows)


if __name__ == "__main__":
    main()
