# Makefile

# Variables
PROJECT_ROOT := /home/Isaac/ViaRail_Project
SRC_DIR := $(PROJECT_ROOT)/src/load
CRED_FILE := $(PROJECT_ROOT)/terraform/gcs_credentials.json
IMAGE := isaacwood111/pyspark_with_packages
SCRIPT := spark_load_staging.py

run:
	docker run -it \
	-v $(CRED_FILE):/tmp/key.json \
	-v $(SRC_DIR):/app \
	-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/key.json \
	$(IMAGE) \
	spark-submit --master local[*] \
	--jars /opt/bitnami/spark/jars/gcs-connector-hadoop3-2.2.5.jar,\
/opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.13-0.40.0.jar \
	/app/$(SCRIPT)
