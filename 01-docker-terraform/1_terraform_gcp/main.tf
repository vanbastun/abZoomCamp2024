terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  credentials = "./keys/creds.json"
  project = "vocal-tempo-411407"
  region  = "europe-central2"
  zone    = "europe-central2-a"
}

resource "google_storage_bucket" "terra-bucket" {
  name          = "vocal-tempo-411407-terra-bucket"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
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
