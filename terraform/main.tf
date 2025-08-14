provider "google" {
  credentials = file("gcs_credentials.json")
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "viar_extracts_datalake" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true
  storage_class = "STANDARD"
}


resource "google_bigquery_dataset" "Via-RailData" {
  dataset_id = var.bq_dataset_name
  location   = var.location
  description = "Production dataset for Via Rail data"
}

resource "google_bigquery_dataset" "ViaRailData_Staging" {
  dataset_id = var.bq_staging_dataset_name
  location   = var.location
  description = "Staging area for Via Rail data before processing"
  }


resource "google_storage_bucket" "via_rail_staging_bucket" {
  name          = var.bq_staging_bucket_name
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true
  storage_class = var.gcs_storage_class
}