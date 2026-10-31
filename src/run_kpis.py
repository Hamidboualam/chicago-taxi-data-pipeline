import os

import duckdb

SQL_FILE = "/opt/airflow/sql/kpis.sql"


def main():
    con = duckdb.connect()

    # Connexion de DuckDB au stockage S3 (RustFS)
    endpoint = os.environ["S3_ENDPOINT"].replace("http://", "")
    con.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{os.environ["S3_ACCESS_KEY"]}',
            SECRET '{os.environ["S3_SECRET_KEY"]}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)

    # Exécute les requêtes du fichier une par une et affiche les résultats
    with open(SQL_FILE, encoding="utf-8") as file:
        queries = file.read().split(";")

    for query in queries:
        if query.strip():
            print(query.strip().splitlines()[0])
            con.sql(query).show()


if __name__ == "__main__":
    main()
