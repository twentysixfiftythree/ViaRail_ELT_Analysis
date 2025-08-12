from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import xxhash64, abs


def build_staging_tables(spark, df):
    """Builds staging tables from the raw data in the JSON files."""
    df = df.withColumn("time_elements", explode(df.times))
    df.show(5, truncate=True)
    #creating id column that combines instance (date of train departure) and train_id.
    #replacing spaces with underscores in the train_instance_id for parsing later
    df = df.withColumn(
        "train_instance_id",
        abs(xxhash64("instance", "train_id"))  # Returns LongType, no cast needed
    )
    df = df.select(
        "train_id", "from", "to", "arrived", "departed",
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
        col('collected_at').alias('collected_at'),
        col('lat').alias('lattitude'),
        col('lng').alias('longitude'),
        col('speed').alias('speed'),
        col('direction').alias('direction'),
    )
    df.show(5, truncate=False)




    distinct_stops = df.select(
    "train_id", "train_instance_date", "station", "code", "arrival_scheduled", "departure_scheduled", "train_instance_id"
    ).dropDuplicates(["train_instance_id", "code"]).orderBy("train_instance_id", "arrival_scheduled")
    df.show(150, truncate=False)

    # add stop number
    distinct_stops.registerTempTable("distinct_stops")
    distinct_stops = spark.sql("""
    SELECT train_id, train_instance_date, station, code, arrival_scheduled, departure_scheduled, train_instance_id,
              ROW_NUMBER() OVER (PARTITION BY train_instance_id ORDER BY arrival_scheduled) AS stop_number, CONCAT(train_id, stop_number) AS stop_id
    FROM distinct_stops
    """)



    distinct_stops.registerTempTable("distinct_stops")
    df.registerTempTable("train_times")
    df = spark.sql("""
    SELECT t.train_id, t.from, t.to, t.arrived, t.departed, t.train_instance_date,
              t.station, t.code, t.estimated,
              t.scheduled, t.eta, t.diff, t.diffMin,
              t.departure_estimated, t.departure_scheduled, t.arrival_eta, t.arrival_scheduled,
              t.train_instance_id, t.collected_at, t.lattitude, t.longitude, t.speed, t.direction, s.stop_number AS stop_number FROM train_times t
              LEFT JOIN distinct_stops s ON t.train_instance_id = s.train_instance_id AND t.code = s.code""")
    df.show(150, truncate=True)

    df.registerTempTable("train_times_staging")



    train_dim = spark.sql(
        """SELECT DISTINCT train_id, `from` AS from_loc, `to` AS to_loc""")

    train_positions_dim = df.select(
        "train_id",
        col("train_instance_date").alias("train_instance_date"),
        abs(xxhash64("train_instance_date", "train_id")).alias("train_instance_id"),
        col("collected_at").alias("collected_at"),
        col('lattitude').alias('latitude'),
        col('longitude').alias('longitude'),
        col('speed'),
        col('direction'),
    ).filter(col('lattitude').isNotNull() & col('longitude').isNotNull())

    #register df table
    df.registerTempTable("train_positions_dim")
    station_dim = spark.sql(
    """
    SELECT DISTINCT code, station FROM train_times_staging
    """).show(5, truncate=False)

    train_time_fact = spark.sql(
    """
    SELECT t.train_id, t.arrived, t.departed, t.train_instance_date, t.estimated,
              t.scheduled, t.eta, t.diff, t.diffMin,
              t.departure_estimated, t.departure_scheduled, t.arrival_eta, t.arrival_scheduled,
              t.train_instance_id, t.collected_at,
              ABS(XXHASH64(CONCAT(CAST(t.train_id AS STRING), CAST(stop_number AS STRING))) % 100000000) AS train_stop_id
              FROM train_times_staging t
              LEFT JOIN distinct_stops s ON t.train_instance_id = s.train_instance_id AND t.code = s.code

    """

    ).show(5, truncate=False)
    #grab time aspects from train_instance date spark sql
    time_dim = spark.sql("""
        SELECT train_instance_date, 
               DATE_FORMAT(train_instance_date, 'yyyy') AS year,
               DATE_FORMAT(train_instance_date, 'MM') AS month,
               DATE_FORMAT(train_instance_date, 'dd') AS day,
               DATE_FORMAT(train_instance_date, 'EEEE') AS day_of_week FROM train_times_staging
    """)

    route_stop_dim = spark.sql("""
        SELECT DISTINCT
            train_id,
            code as station_code,
            stop_number,
            ABS(XXHASH64(CONCAT(CAST(train_id AS STRING), CAST(stop_number AS STRING))) % 100000000) AS train_stop_id
        FROM train_times_staging
        ORDER BY train_id, stop_number
    """)

    route_info_dim.show(10, truncate=False)


def move_processed_json_files():

    pass

def main():
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('ViaRailProcessor') \
        .set("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/tmp/key.json")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    print("✅ Spark session created")
    
    df = spark.read.json("gs://viarail-json-datalake/datafiles/*.json")
    # df.printSchema()  # Shows the inferred schema
    # print some rows
    build_staging_tables(spark, df)



if __name__ == "__main__":
    main()