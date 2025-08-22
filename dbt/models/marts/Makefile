PROJECT_ROOT := /home/Isaac/ViaRail_Project
MARTS_DIR := /dbt/models/marts
GCS_CREDS := $(PROJECT_ROOT)/terraform/gcs_credentials.json
DBT_IMAGE := ghcr.io/dbt-labs/dbt-bigquery:1.8.2

define DBT_RUN
	docker run --network=host \
		--mount type=bind,source=$(PROJECT_ROOT)/dbt,target=/usr/app \
		--mount type=bind,source=/home/Isaac/.dbt,target=/root/.dbt \
		--mount type=bind,source=$(GCS_CREDS),target=/tmp/key.json \
		-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/key.json \
		-w /usr/app \
		$(DBT_IMAGE)
endef
#

run-core:
	$(DBT_RUN) run --select path:models/core


run-marts:
	$(DBT_RUN) run --select path:models/marts


run-all:
	$(DBT_RUN) run