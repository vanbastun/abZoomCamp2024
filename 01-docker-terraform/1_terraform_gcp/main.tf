terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  credentials = "./keys/creds.json"
  project     = var.project
  region      = var.region
  zone        = var.zone
}

resource "google_storage_bucket" "terra-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "nytaxi_dataset" {
  dataset_id = var.google_bigquery_dataset_name
  location = var.location
}
