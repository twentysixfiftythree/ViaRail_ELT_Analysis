variable "project" {
  description = "GCP project ID"
  default     = "elt-viarail"
}
variable "region" {
  description = "GCP region"
  default     = "us-central1"
}

variable "location" {
  description = "project location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My bigquery dataset name"
  default     = "viarail_dataset"
}

variable "bq_staging_dataset_name" {
  description = "My bigquery staging dataset name"
  default     = "viarail_staging_dataset"
}

variable "gcs_bucket_name" {
  description = "my storage bucket name"
  default     = "viarail-json-datalake"
}

variable "bq_staging_bucket_name" {
  description = "my staging bucket name"
  default     = "viarail-staging-bucket"
}
variable "gcs_storage_class" {
  description = "my storage class"
  default     = "STANDARD"
}