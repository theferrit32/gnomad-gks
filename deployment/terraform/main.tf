terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = "kferrite-sandbox-26c7"
  region  = "us-central1"
  zone    = "us-central1-a"
}

data "google_compute_network" "sandbox_network" {
  name = "kferrite-sandbox-network"
}

# ─── Artifact Registry ──────────────────────────────────────────
resource "google_artifact_registry_repository" "gnomad_gks" {
  location      = "us-central1"
  repository_id = "gnomad-gks"
  description   = "Docker images for gnomAD VRS annotation Cloud Run jobs"
  format        = "DOCKER"

  labels = {
    managed-by = "terraform"
    created-by = "kyle"
  }
}

# ─── Service Account for Cloud Run Jobs ─────────────────────────
resource "google_service_account" "cloudrun_gnomad" {
  account_id   = "cloudrun-gnomad"
  display_name = "Cloud Run gnomAD VRS annotation jobs"
}

resource "google_storage_bucket_iam_member" "cloudrun_gnomad_writer" {
  bucket = "kferrite-sandbox-storage"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.cloudrun_gnomad.email}"
}

resource "google_project_iam_member" "cloudrun_gnomad_ar_reader" {
  project = "kferrite-sandbox-26c7"
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.cloudrun_gnomad.email}"
}

# ─── Compute Engine Instance ────────────────────────────────────
resource "google_compute_instance" "gnomad_gks_instance" {
  name         = "gnomad-gks"
  machine_type = "c4a-standard-2"

  labels = {
    managed-by = "terraform"
    created-by = "kyle"
  }

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-minimal-2510-arm64"
      size  = 200
      type  = "hyperdisk-balanced"
    }
  }

  network_interface {
    network = data.google_compute_network.sandbox_network.id
    access_config {}
  }
}
