# Chicago Taxi Data Pipeline

Pipeline de données de bout en bout sur les trajets de taxi de Chicago : ingestion, nettoyage, agrégations et exposition pour la BI. Tout tourne en local avec Docker Compose.

## Lancer le projet

### Prérequis

- Docker et Docker Compose
- Une connexion internet (les données sont téléchargées depuis l'API de la ville de Chicago)
- Les ports 8080, 8501, 9000 et 9001 libres

Testé sous Windows 11 avec Docker 29 et 10 Go de RAM alloués à Docker.

### Pas à pas

**1. Cloner le repo et démarrer les services**

```bash
git clone https://github.com/Hamidboualam/chicago-taxi-data-pipeline.git
cd chicago-taxi-data-pipeline
docker compose up -d --build
```

Le premier lancement prend quelques minutes, le temps de construire les images. Airflow met ensuite environ 1 minute à démarrer.

**2. Lancer le pipeline**

Ouvrir Airflow sur http://localhost:8080 (`admin` / `admin`), puis cliquer sur ▶ à droite du DAG `taxi_pipeline`.

Ou en ligne de commande :

```bash
docker compose exec airflow airflow dags trigger taxi_pipeline
```

Le pipeline complet prend environ 10 à 30 minutes. C'est surtout le téléchargement : sa durée dépend de l'API.

**3. Voir les résultats**

- Dashboard : http://localhost:8501
- Requêtes SQL dans le terminal : `docker compose exec airflow python /opt/airflow/src/run_kpis.py`
- Fichiers du datalake : http://localhost:9001/rustfs/console/index.html (`rustfsadmin` / `rustfsadmin`)

### Autres commandes

```bash
# Lancer les tests
docker compose exec airflow pytest /opt/airflow/tests

# Arrêter les services (les données sont conservées)
docker compose stop

# Tout supprimer, y compris les données du datalake
docker compose down -v
```

## Architecture

```
  API SODA (ville de Chicago)
            │
            │  ingest_bronze.py : téléchargement par pages
            ▼
  ┌───────────────────┐
  │      BRONZE       │  CSV bruts, non modifiés, par année et mois
  └─────────┬─────────┘
            │  bronze_to_silver.py (Spark) : typage, doublons, nettoyage
            ▼
  ┌───────────────────┐
  │      SILVER       │  Parquet nettoyé, par année et mois
  └─────────┬─────────┘
            │  silver_to_gold.py (Spark) : jointure et agrégations
            ▼
  ┌───────────────────┐
  │       GOLD        │  3 tables Parquet prêtes pour la BI
  └─────────┬─────────┘
            │  check_gold.py : contrôle final
            ▼
  DuckDB : requêtes SQL et dashboard Streamlit
```

Les 3 couches sont stockées dans le bucket `datalake` de RustFS (stockage objet compatible S3). Airflow enchaîne les étapes avec un seul DAG :

```
ingest_bronze >> bronze_to_silver >> silver_to_gold >> check_gold
```

### Organisation du repo

```
├── dags/taxi_pipeline.py     # le DAG Airflow
├── src/
│   ├── ingest_bronze.py      # API vers bronze
│   ├── bronze_to_silver.py   # bronze vers silver
│   ├── silver_to_gold.py     # silver vers gold
│   ├── check_gold.py         # contrôle des tables gold
│   ├── run_kpis.py           # exécute les requêtes SQL
│   └── spark_session.py      # session Spark connectée à RustFS
├── sql/kpis.sql              # requêtes pour la BI
├── dashboard/                # dashboard Streamlit
├── data/community_areas.csv  # noms des 77 zones de Chicago
├── tests/test_silver.py      # tests des règles de nettoyage
├── Dockerfile                # image Airflow avec Java, PySpark et DuckDB
└── docker-compose.yml
```

## Les données

**Source :** [Chicago Taxi Trips](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew). Les noms des zones viennent des [limites des community areas](https://data.cityofchicago.org/resource/igwz-8jzy.csv) du même portail.

**Période choisie :** premier trimestre 2023 (1er janvier au 31 mars), soit 1 446 439 trajets et environ 644 Mo de CSV. C'est un volume assez gros pour justifier Spark, mais qui reste exploitable en local.

**Téléchargement :** le script [src/ingest_bronze.py](src/ingest_bronze.py) filtre sur `trip_start_timestamp` et récupère les données mois par mois, par pages de 50 000 lignes. Le DAG le lance automatiquement. Pour le lancer seul :

```bash
docker compose exec airflow python /opt/airflow/src/ingest_bronze.py
```

La période se change dans la liste `PERIODS` du script. L'option `--max-rows 10000` limite le nombre de lignes par mois pour faire un test rapide, mais elle remplace les données déjà présentes en bronze.

### Règles de nettoyage (silver)

J'ai choisi ces règles après avoir analysé les données. Une ligne est gardée seulement si :

| Donnée | Règle | Pourquoi |
|---|---|---|
| `trip_id` | Unique et non vide | Identifie le trajet |
| Dates | Fin après ou égale au début | 9 lignes ont une fin avant le début |
| `trip_seconds` | De 1 seconde à 6 h | Certains compteurs restent allumés jusqu'à 24 h |
| `trip_miles` | De 0 à 100 miles | Certaines valeurs montent à 899 miles |
| `fare` | De 0 à 500 $ | Certaines valeurs sont à 9 999,75 $ |
| `trip_total` | Plus de 0 et jusqu'à 500 $ | Même problème, et un total à 0 n'apporte rien aux revenus |

Ces seuils sont des choix de ma part, pas des valeurs officielles. Au final, 32 088 lignes sont rejetées (2,22 %) et 1 414 351 sont gardées. Le script s'arrête en erreur si la table est vide ou si plus de 10 % des lignes sont rejetées.

Je garde volontairement les trajets à 0 mile (126 326) et les trajets sans zone (84 206), car leur durée et leur prix restent exploitables. Par contre, la distance n'est pas assez fiable pour en faire un KPI, et les trajets sans zone sont exclus du top des zones.

### Tables gold

| Table | Contenu |
|---|---|
| `revenue_per_day` | Nombre de trajets et revenu par jour |
| `avg_trip_duration_per_day` | Durée moyenne des trajets par jour |
| `top_pickup_zones` | Les 10 zones avec le plus de prises en charge, avec leur nom |

### Quelques résultats

- 36,3 M$ de revenu sur le trimestre, soit 25,70 $ par trajet en moyenne.
- O'Hare (l'aéroport) représente environ 17 % des trajets et 34 % du revenu, avec 52,41 $ par trajet contre environ 15 $ pour Near North Side et Loop.

## Choix techniques et arbitrages

- **Airflow en mode `standalone` :** un seul conteneur, simple à lancer. Ce mode n'est pas fait pour la production, et les tâches s'exécutent l'une après l'autre, ce qui suffit pour ce DAG linéaire.
- **Spark en mode local :** pas de cluster, Spark tourne dans le conteneur Airflow. Une seule machine suffit pour 1,4 million de lignes, et le même code pourrait tourner sur un cluster.
- **RustFS plutôt que MinIO :** je voulais utiliser MinIO, mais ses images Docker n'étaient plus téléchargeables quand j'ai commencé. RustFS est compatible S3 et open source, donc Spark et DuckDB l'utilisent comme n'importe quel stockage S3.
- **CSV en bronze, Parquet ensuite :** le bronze garde la donnée telle qu'elle arrive. Parquet est plus léger (78 Mo en silver) et plus rapide à lire, même si une partie du gain vient des colonnes retirées. Je n'ai pas utilisé Delta Lake ou Iceberg, car je n'ai pas besoin de leurs fonctionnalités ici.
- **Tri de la pagination :** l'API a besoin d'un tri stable pour ne pas perdre de lignes entre deux pages. Le tri sur `trip_id` seul était trop lent (rien après 100 secondes), alors que `trip_start_timestamp,trip_id` a pris 13 secondes pour 10 000 lignes.
- **Relancer sans doublons :** le bronze supprime les anciens fichiers du mois avant de les remplacer, et le silver et le gold sont écrits en mode `overwrite`. L'inconvénient est que tout est retraité à chaque lancement.
- **Lancement manuel du DAG :** la période de données est fixe, donc un lancement planifié n'apporterait rien.
- **DuckDB et Streamlit pour l'exposition :** ils lisent directement les fichiers Parquet, sans base de données à gérer.
- **Identifiants simples dans le code :** ils ne servent qu'en local. Pour les changer, il suffit de copier `.env.example` en `.env`.

## Ce que j'aurais fait avec plus de temps

- Passer à une ingestion incrémentale, pour ne pas tout retélécharger à chaque lancement.
- Ajouter plus de contrôles qualité, par exemple avec Great Expectations.
- Garder les lignes rejetées dans une table à part, pour pouvoir les analyser.
- Utiliser Airflow avec Postgres et Spark sur un vrai cluster, pour de plus gros volumes.
- Passer le silver en Delta Lake ou Iceberg, pour l'historique et les mises à jour.
- Ajouter de la CI (tests automatiques à chaque push) et des alertes en cas d'échec du DAG.
- Ajouter des filtres au dashboard (période, zone, entreprise).
