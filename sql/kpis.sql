-- Résumé du trimestre
SELECT
    SUM(trip_count) AS trips,
    ROUND(SUM(revenue), 2) AS revenue,
    ROUND(SUM(revenue) / SUM(trip_count), 2) AS revenue_per_trip
FROM read_parquet('s3://datalake/gold/revenue_per_day/*.parquet');

-- Revenus par mois
SELECT
    MONTH(trip_date) AS month,
    SUM(trip_count) AS trips,
    ROUND(SUM(revenue), 2) AS revenue
FROM read_parquet('s3://datalake/gold/revenue_per_day/*.parquet')
GROUP BY month
ORDER BY month;

-- Les 5 jours avec le plus de revenus
SELECT trip_date, trip_count, revenue
FROM read_parquet('s3://datalake/gold/revenue_per_day/*.parquet')
ORDER BY revenue DESC
LIMIT 5;

-- Les 5 jours avec les trajets les plus longs en moyenne
SELECT trip_date, avg_duration_minutes
FROM read_parquet('s3://datalake/gold/avg_trip_duration_per_day/*.parquet')
ORDER BY avg_duration_minutes DESC
LIMIT 5;

-- Top des zones de prise en charge
SELECT
    community_name,
    trip_count,
    revenue,
    ROUND(revenue / trip_count, 2) AS revenue_per_trip
FROM read_parquet('s3://datalake/gold/top_pickup_zones/*.parquet')
ORDER BY trip_count DESC;
