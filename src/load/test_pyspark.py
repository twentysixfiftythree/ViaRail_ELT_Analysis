from pyspark.sql import SparkSession
from pyspark import SparkConf

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
    print(f"📊 Found {df.count()} records")
    print(df.columns)
    df.printSchema()  # Shows the inferred schema
    df.show(5, truncate=True)  # Shows sample data
    spark.stop()

if __name__ == "__main__":
    main()