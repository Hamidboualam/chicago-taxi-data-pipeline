import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from bronze_to_silver import cast_columns, clean_trips

VALID_TRIP = {
    "trip_id": "a",
    "taxi_id": "taxi1",
    "trip_start_timestamp": "2023-01-01T10:00:00.000",
    "trip_end_timestamp": "2023-01-01T10:15:00.000",
    "trip_seconds": "900",
    "trip_miles": "3.5",
    "pickup_community_area": "8",
    "dropoff_community_area": "32",
    "fare": "12.5",
    "tips": "2",
    "tolls": "0",
    "extras": "1",
    "trip_total": "15.5",
    "payment_type": "Cash",
    "company": "Flash Cab",
    "year": "2023",
    "month": "1",
}


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()


def clean(spark, trips):
    """Passe des trajets (en texte, comme dans le CSV) dans les fonctions du silver."""
    schema = StructType([StructField(name, StringType()) for name in VALID_TRIP])
    rows = [[trip[name] for name in VALID_TRIP] for trip in trips]
    return clean_trips(cast_columns(spark.createDataFrame(rows, schema))).collect()


def test_valid_trip_is_kept_with_right_types(spark):
    result = clean(spark, [VALID_TRIP])
    assert len(result) == 1
    assert result[0].trip_seconds == 900
    assert result[0].trip_total == 15.5


def test_duplicates_are_removed(spark):
    assert len(clean(spark, [VALID_TRIP, VALID_TRIP])) == 1


@pytest.mark.parametrize(
    "column, bad_value",
    [
        ("trip_id", None),
        ("trip_end_timestamp", "2023-01-01T09:00:00.000"),
        ("trip_seconds", "0"),
        ("trip_seconds", "30000"),
        ("trip_miles", "500"),
        ("fare", "9999.75"),
        ("trip_total", "0"),
        ("trip_total", "9999.75"),
        ("trip_total", None),
    ],
)
def test_incoherent_trip_is_rejected(spark, column, bad_value):
    assert clean(spark, [{**VALID_TRIP, column: bad_value}]) == []


def test_trip_with_zero_miles_and_no_zone_is_kept(spark):
    trip = {**VALID_TRIP, "trip_miles": "0", "pickup_community_area": None}
    assert len(clean(spark, [trip])) == 1
