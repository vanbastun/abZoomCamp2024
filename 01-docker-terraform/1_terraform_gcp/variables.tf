variable "project" {
  description = "Project"
  default = "vocal-tempo-411407"
}

variable "location" {
  description = "Project location"
  default = "EU"
}

variable "region" {
  description = "Project region"
  default = "europe-central2"
}

variable "zone" {
  description = "Project zone"
  default = "europe-central2-a"
}

variable "google_bigquery_dataset_name" {
  description = "My dataset name"
  default = "nytaxi_dataset"
}

variable "gcs_bucket_name" {
  description = "Bucket storage name"
  default = "ab-zoomcamp2024-green-taxi-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default = "STANDART"
}
