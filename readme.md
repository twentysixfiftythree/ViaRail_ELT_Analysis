
## What this is

This is a personal data engineering project I built mostly for fun -- and out of frustration -- to analyze how consistently delayed ViaRail trains are. I wanted to turn my annoyance into something mildly productive, and this ended up being a full-stack ELT-LT data pipeline from ingestion to dashboard.
Every few minutes, the system fetches live train movement data from ViaRail’s (not-so-documented) API, loads it into a BigQuery warehouse, models it with DBT, and displays insights in Looker Studio.
The main questions I’m exploring:
 - How often are trains delayed?
 - Which routes are the worst offenders?
 - How bad are delays at specific stations?

You can explore the live data here:

[🔗 Looker Studio Dashboard](https://lookerstudio.google.com/reporting/58473347-2c18-4bd1-aa61-9e12f901712b)



The project is broken down into a few steps listed here.

## Steps

0. Used Terraform to build the GCP infastructure, including buckets for the datalake, and staging/prod datasets in BigQuery.
1. I used Kestra, which is a data orchestration tool, to run a shell script every 5 minutes that grabs the JSON file from ViaRail's "API" and puts them into my datalake in Google Cloud Storage. 
  It also does some very minimal preprocessing like adding a collected_at feature.
    - ViaRail posts live, mostly complete, data every few minutes including all current train routes, their stops, and timing information.
2. Even though Spark might seem overkill for this amount of data I'm currently working with (a few million rows), I believe it's justifiable since the data grows very, very quickly.
    - Spark processes the raw JSONS and builds them into a Star Schema data warehouse with the proper fact and dim tables detailed below.
    - It's then loaded into the staging dataset in BigQuery.
    - A mixture of the ORM and pure SQL were used to build this. This is because I like writing window functions, and a few other things, in pure SQL.
3. Using incremental models in DBT, the fact and dim tables are created/updated in the "prod" dataset.
4. Data marts are created for stop-by-stop performance, and end-to-end train delay analysis.



## Star Schema Overview
Fact Tables
    train_position_fact: Actual positions and delays
    time_fact: Delay time calculations
Dim Tables
    train_dim: Train identifiers and metadata
    station_dim: Station names and codes
    route_stop_dim: Sequence of stops per route
    train_time_dim: Scheduled and actual times


<img width="803" height="590" alt="Screenshot 2025-08-24 at 8 44 33 AM" src="https://github.com/user-attachments/assets/152b744a-25ce-436b-bbd0-954b69ea919a" />

## Running Steps


In order to see any real meaningful data, I would recommend letting the shell collection script run for a few days.
0. Make sure you have terraform and docker installed and working.
1. Fix the terraform files to match your GCP.
2. Run terraform init
3. Run terraform plan (optional, but useful for debugging any problems)
4. Run terraform apply
5. Run docker-compose up in the project directory.
6. Copy the flow from the 'flow' folder into your own flow in Kestra, and specify your GCP credentials in the Kestra KV store.
7. Alter the Makefiles to specify your own directories and credentials.
8. Run this Cronjob and configure it to run as often as you'd like your BigQuery data updated:
  - ```{bash}
    0 0 * * 0 cd {your_path}/ViaRail_Project/src/load && make run && cd {your_path}/ViaRail_Project/dbt/models/marts && make run-all





## Dashboard Preview


<img width="1206" height="923" alt="Screenshot 2025-08-24 at 9 01 27 AM" src="https://github.com/user-attachments/assets/f1c4de29-b005-4a2c-bda5-efc015032822" />
<img width="1206" height="923" alt="Screenshot 2025-08-24 at 9 05 10 AM" src="https://github.com/user-attachments/assets/514b5ceb-b8bd-41b2-9347-42f21039203e" />
