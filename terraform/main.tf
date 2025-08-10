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
}