from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
    df.show(5, truncate=True)

    #explode times
    df = df.withColumn("time_elements", explode(df.times))
    df.show(5, truncate=True)
    #creating id column that combines instance (date of train departure) and train_id.
    #replacing spaces with underscores in the train_instance_id for parsing later
    df= df.withColumn("train_instance_id", F.concat(df.instance, F.lit("__"), df.train_id)).withColumn('train_instance_id', F.regexp_replace('train_instance_id', ' ', '_'))

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
        col("train_instance_id").alias("train_instance_id")
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
              ROW_NUMBER() OVER (PARTITION BY train_instance_id ORDER BY arrival_scheduled) AS stop_number
    FROM distinct_stops
    """)
    distinct_stops.registerTempTable("distinct_stops")
    df.registerTempTable("train_times")
    df = spark.sql("""
    SELECT t.train_id, t.from, t.to, t.arrived, t.departed, t.train_instance_date,
              t.station, t.code, t.estimated,
              t.scheduled, t.eta, t.diff, t.diffMin,
              t.departure_estimated, t.departure_scheduled, t.arrival_eta, t.arrival_scheduled,
              t.train_instance_id, s.stop_number FROM train_times t
              LEFT JOIN distinct_stops s ON t.train_instance_id = s.train_instance_id""")
    df.show(150, truncate=False)


if __name__ == "__main__":
    main()