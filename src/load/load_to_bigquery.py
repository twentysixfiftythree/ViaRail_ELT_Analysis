# inherits needed libraries from spark_load_staging.py


class BigQueryLoader:
    def __init__(self, spark, tables):
        """
        Initialize the BigQueryLoader with Spark session and raw data.

        :param spark: Spark session
        :param raw_data: Raw data to be processed
        """
        self.spark = spark
        self.tables = tables
        # reduce output
        self.spark.sparkContext.setLogLevel("WARN")
        self.tempbucket = "viarail-staging-bucket"

    def load_staging_to_bigquery(self):
        # ----------
        # Load train time fact table
        # ----------

        self.tables["train_time_fact"].write.format("bigquery").option(
            "table", "elt-viarail:viarail_staging_dataset.train_time_fact"
        ).option("parentProject", "elt-viarail").option(
            "credentialsFile", "/tmp/key.json"
        ).option(
            "partitionField", "train_instance_date"
        ).option(
            "clusteredFields", "train_instance_id"
        ).mode(
            "overwrite"
        ).save()

        print("Fact time table data loaded to BigQuery staging table successfully.")

        # --------
        # Load train dimension tables
        # --------

        self.tables["train_dim"].write.format("bigquery").option(
            "table", "elt-viarail:viarail_staging_dataset.train_dim"
        ).option("parentProject", "elt-viarail").option(
            "credentialsFile", "/tmp/key.json"
        ).mode(
            "overwrite"
        ).save()
        print("Train dim data loaded to BigQuery staging table successfully.")

        # --------
        # load train position fact table
        # --------

        self.tables["train_positions_fact"].write.format("bigquery").option(
            "table", "elt-viarail:viarail_staging_dataset.train_position_fact"
        ).option("parentProject", "elt-viarail").option(
            "credentialsFile", "/tmp/key.json"
        ).option(
            "partitionField", "train_instance_date"
        ).mode(
            "overwrite"
        ).save()
        print("Fact table data loaded to BigQuery staging table successfully.")

        # --------
        # load station dimension table
        # --------

        self.tables["station_dim"].write.format("bigquery").option(
            "table", "elt-viarail:viarail_staging_dataset.station_dim"
        ).option("parentProject", "elt-viarail").option(
            "credentialsFile", "/tmp/key.json"
        ).mode(
            "overwrite"
        ).save()
        print(
            "Station dimension table data loaded to BigQuery staging table successfully."
        )

        # ----------
        # load train time dimension table
        # ----------
        self.tables["time_dim"].write.format("bigquery").option(
            "table", "elt-viarail:viarail_staging_dataset.train_time_dim"
        ).option("parentProject", "elt-viarail").option(
            "credentialsFile", "/tmp/key.json"
        ).mode(
            "overwrite"
        ).save()
        print(
            "Train time dimension table data loaded to BigQuery staging table successfully."
        )

        # --------
        # load route stop dimension table
        # --------

        self.tables["route_stop_dim"].write.format("bigquery").option(
            "table", "elt-viarail:viarail_staging_dataset.route_stop_dim"
        ).option("parentProject", "elt-viarail").option(
            "credentialsFile", "/tmp/key.json"
        ).mode(
            "overwrite"
        ).save()
        print(
            "Route stop dimension table data loaded to BigQuery staging table successfully."
        )
