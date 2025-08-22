from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import xxhash64, abs
from google.cloud import storage
from process_lake import move_files_to_processed
from load_to_bigquery import BigQueryLoader
from pyspark.sql.functions import split, concat_ws, to_timestamp


class BatchProcessor:
    def __init__(self, spark, raw_data):
        self.spark = spark
        self.raw_data = raw_data
        self.tables = {}
        # reduce output
        self.spark.sparkContext.setLogLevel("WARN")

    def build_prestaging_fact(self, df):
        """Process the DataFrame and build staging tables. This dataframe is used to build the fact table and dimension tables."""
        df = (
            df.withColumn("time_elements", explode(df.times))
            .withColumn("train_instance_id", abs(xxhash64("instance", "train_id")))
            .select(
                "train_id",
                "from",
                "to",
                "arrived",
                "departed",
                col("collected_at_t").alias("collected_at_t"),
                col("instance").alias("train_instance_date"),
                col("time_elements.station").alias("station"),
                col("time_elements.code").alias("code"),
                col("time_elements.estimated").alias("estimated"),
                col("time_elements.scheduled").alias("scheduled"),
                col("time_elements.eta").alias("eta"),
                col("time_elements.diff").alias("diff"),
                col("time_elements.diffMin").alias("diffMin"),
                col("time_elements.departure.estimated").alias("departure_estimated"),
                col("time_elements.departure.scheduled").alias("departure_scheduled"),
                col("time_elements.arrival.estimated").alias("arrival_eta"),
                col("time_elements.arrival.scheduled").alias("arrival_scheduled"),
                col("train_instance_id").alias("train_instance_id"),
                col("lat").alias("latitude"),
                col("lng").alias("longitude"),
                col("speed").alias("speed"),
                col("direction").alias("direction"),
            )
        ).cache()  # Cache the main working DataFrame
        distinct_stops = (
            df.select(
                "train_id",
                "train_instance_date",
                "station",
                "code",
                "arrival_scheduled",
                "departure_scheduled",
                "train_instance_id",
            )
            .dropDuplicates(["train_instance_id", "code"])
            .orderBy("train_instance_id", "arrival_scheduled")
        )
        # add stop number
        distinct_stops.createOrReplaceTempView("distinct_stops")
        distinct_stops = self.spark.sql(
            """
        SELECT train_id, train_instance_date, station, code, arrival_scheduled, departure_scheduled, train_instance_id,
                ROW_NUMBER() OVER (PARTITION BY train_instance_id ORDER BY arrival_scheduled) AS stop_number
        FROM distinct_stops
        """
        ).cache()
        distinct_stops.createOrReplaceTempView("distinct_stops")

        df.createOrReplaceTempView("train_times")
        df = self.spark.sql(
            """
        SELECT t.train_id, t.`from`, t.`to`, t.arrived, t.departed, t.train_instance_date,
              t.station, t.code, t.estimated,
              t.scheduled, t.eta, t.diff, t.diffMin,
              t.departure_estimated, t.departure_scheduled, t.arrival_eta, t.arrival_scheduled,
              t.train_instance_id, t.collected_at_t, t.latitude, t.longitude, t.speed, t.direction, s.stop_number AS stop_number FROM train_times t
              LEFT JOIN distinct_stops s ON t.train_instance_id = s.train_instance_id AND t.code = s.code"""
        )

        df.createOrReplaceTempView("train_times_staging")
        return df

    def build_dim_tables(self):
        # -----------------
        # Train dimension table: Grab distinct train IDs and their from/to locations
        # -----------------
        train_dim = self.spark.sql(
            """SELECT DISTINCT train_id, `from` AS from_loc, `to` AS to_loc FROM train_times_staging"""
        )
        # -----------------
        # Train positions dimension table: Extract train positions with non-null latitude and longitude (trains that have arrived don't have positions)
        # -----------------
        train_positions_fact = self.spark.sql(
            """
        SELECT DISTINCT
            ABS(
                XXHASH64(
                    CONCAT(
                        CAST(ABS(XXHASH64(CONCAT(CAST(train_instance_id AS STRING), CAST(stop_number AS STRING)))) AS STRING),
                        CAST(collected_at_t AS STRING)
                    )
                )
            ) AS train_stop_record_id,
            train_instance_id, train_id, CAST(train_instance_date AS DATE),
                   collected_at_t, latitude, longitude, speed, direction
            FROM train_times_staging
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """
        )

        # train id
        # -----------------
        # Station dimension table: Extract distinct station codes and names
        # -----------------

        station_dim = self.spark.sql(
            """
        SELECT DISTINCT code, station FROM train_times_staging
        """
        )
        # -----------------
        # Time dimension table: extracting time aspects from train_instance_date
        # -----------------

        time_dim = self.spark.sql(
            """
        SELECT DISTINCT train_instance_date, 
               DATE_FORMAT(train_instance_date, 'yyyy') AS year,
               DATE_FORMAT(train_instance_date, 'MM') AS month,
               DATE_FORMAT(train_instance_date, 'dd') AS day,
               DATE_FORMAT(train_instance_date, 'EEEE') AS day_of_week FROM train_times_staging
    """
        )
        # -----------------
        # Route stop dimension table: Create a unique identifier for each train stop/train id combination
        # This is used to link the train stops to the train time fact table
        # -----------------
        route_stop_dim = self.spark.sql(
            """
            SELECT DISTINCT
                train_id,
                code as station_code,
                stop_number,
                ABS(XXHASH64(CONCAT(CAST(train_id AS STRING), CAST(stop_number AS STRING), CAST(code AS STRING)))) AS train_stop_id
            FROM train_times_staging
        """
        )
        self.tables = {
            "train_dim": train_dim,
            "train_positions_fact": train_positions_fact,
            "station_dim": station_dim,
            "time_dim": time_dim,
            "route_stop_dim": route_stop_dim,
        }

    def build_fact_table(self):
        train_time_fact = self.spark.sql(
            """
            SELECT
                    ABS(
                        XXHASH64(
                        CONCAT(
                        CAST(ABS(XXHASH64(CONCAT(CAST(train_instance_id AS STRING), CAST(stop_number AS STRING)))) AS STRING),
                        CAST(collected_at_t AS STRING)
                        )
                    )
                ) AS train_stop_record_id,
                train_id,
                arrived,
                departed,
                CAST (train_instance_date AS DATE) AS train_instance_date,
                estimated,
                CAST(scheduled AS TIMESTAMP) AS scheduled,
                eta,
                diff,
                diffMin,
                CAST(departure_estimated AS TIMESTAMP) AS departure_estimated,
                CAST(departure_scheduled AS TIMESTAMP) AS departure_scheduled,
                CAST(arrival_eta AS TIMESTAMP) AS arrival_eta,
                CAST(arrival_scheduled AS TIMESTAMP) AS arrival_scheduled,
                train_instance_id,
                CAST(collected_at_t AS TIMESTAMP) AS collected_at,
                ABS(XXHASH64(CONCAT(CAST(train_id AS STRING), CAST(stop_number AS STRING), CAST(code AS STRING)))) AS train_stop_id
            FROM train_times_staging
            """
        )

        self.tables["train_time_fact"] = train_time_fact

    def show_statistics(self):
        """Display statistics for all created tables"""
        print("\n📊 Table Statistics:")
        print("-" * 50)

        for table_name, table_df in self.tables.items():
            table_df.cache()  # Cache before counting
            count = table_df.count()
            print(f"  {table_name:20s}: {count:,} records")

        print("-" * 50)

    def build_all_tables(self):
        """Build all tables with proper error handling and statistics"""
        try:
            print("🚀 Starting batch processing pipeline...")

            prestaging_df = self.build_prestaging_fact(self.raw_data)

            self.build_dim_tables()

            self.build_fact_table()

            # takes too much time
            # self.show_statistics()

            print("🎉 Batch processing completed successfully!")
            return self.tables

        except Exception as e:
            print(f"❌ Error during batch processing: {str(e)}")
            raise

    def configure_spark(self):
        """Apply performance optimizations"""
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set(
            "spark.sql.autoBroadcastJoinThreshold", "104857600"
        )  # 100MB


def main():
    conf = (
        SparkConf()
        .setMaster("local[*]")
        .setAppName("ViaRailProcessor")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .set(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/tmp/key.json",
        )
        .set("spark.hadoop.fs.gs.project.id", "elt-viarail")
        .set("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/tmp/key.json")
        .set("temporaryGcsBucket", "viarail-staging-bucket")
        .set("spark.bigquery.projectId", "elt-viarail")
        .set("spark.bigquery.parentProject", "elt-viarail")
        .set("spark.bigquery.credentialsFile", "/tmp/key.json")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    print("✅ Spark session created")

    # ;oad data from GCS
    raw_data = spark.read.json("gs://viarail-json-datalake/datafiles/*.json")

    raw_data = raw_data.withColumn("parts", split("collected_at", "_"))

    raw_data = raw_data.withColumn(
        "collected_at_t",
        to_timestamp(
            concat_ws(
                " ",
                raw_data["parts"].getItem(0),  # Date part: "2025-08-16"
                concat_ws(
                    ":",  # Time part: "08:30"
                    raw_data["parts"].getItem(1),  # Hour: "08"
                    raw_data["parts"].getItem(2),  # Minute: "30"
                ),
            ),
            "yyyy-MM-dd HH:mm",
        ),
    )
    print("📂 Raw Data Schema:")
    raw_data.printSchema()

    processor = BatchProcessor(spark, raw_data)

    processor.configure_spark()
    print("✅ Spark configured")

    tables = processor.build_all_tables()

    print("\n📌 Sample from fact table:")
    tables["train_time_fact"].show(5, truncate=False)
    print("Using BigQuery project:", spark.conf.get("spark.bigquery.projectId"))
    print(
        "Using billing project (parentProject):",
        spark.conf.get("spark.bigquery.parentProject"),
    )

    loader = BigQueryLoader(spark, tables)
    loader.load_staging_to_bigquery()
    move_files_to_processed("viarail-json-datalake", "datafiles")


if __name__ == "__main__":
    main()
