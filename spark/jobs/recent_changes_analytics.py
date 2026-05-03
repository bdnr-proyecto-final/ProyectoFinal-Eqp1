import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


KEYSPACE = os.getenv("KEYSPACE", "wikimedia")
RAW_TABLE = os.getenv("RAW_TABLE", "recent_changes_raw")
AGG_TABLE = os.getenv("AGG_TABLE", "changes_by_wiki_hour")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
OUTPUT_PATH = os.getenv(
    "ANALYTICS_OUTPUT_PATH", "/tmp/spark-output/changes_by_wiki_hour"
)
WRITE_TO_CASSANDRA = os.getenv("ANALYTICS_WRITE_TO_CASSANDRA", "false").lower() == "true"


def create_spark_session():
    """Crea una sesion de Spark configurada para Cassandra."""
    spark = (
        SparkSession.builder.appName("recent-changes-analytics")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = create_spark_session()

    raw_df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=RAW_TABLE, keyspace=KEYSPACE)
        .load()
    )
    raw_count = raw_df.count()

    cleaned_df = raw_df.dropna(
        subset=["timestamp_event", "wiki", "event_date", "event_hour"]
    )
    cleaned_count = cleaned_df.count()

    aggregated_df = (
        cleaned_df.groupBy("event_date", "wiki", "event_hour", "change_type")
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.sum(F.when(F.col("bot") == True, F.lit(1)).otherwise(F.lit(0))).alias(
                "bot_events"
            ),
        )
        .orderBy("event_date", "wiki", "event_hour", "change_type")
    )
    aggregated_count = aggregated_df.count()

    (
        aggregated_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )

    if WRITE_TO_CASSANDRA:
        (
            aggregated_df.write.format("org.apache.spark.sql.cassandra")
            .options(table=AGG_TABLE, keyspace=KEYSPACE)
            .mode("append")
            .save()
        )
        print(f"[SPARK] Agregados insertados en {KEYSPACE}.{AGG_TABLE}")

    print(f"[SPARK] Filas leidas desde Cassandra: {raw_count}")
    print(f"[SPARK] Filas despues de limpieza: {cleaned_count}")
    print(f"[SPARK] Filas agregadas: {aggregated_count}")
    print(f"[SPARK] Salida generada en: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
