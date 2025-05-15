terraform {
  required_version = ">= 1.5"

  backend "local" {}

  required_providers {
    google = {
      version = ">= 5.36.0"
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = "bigquerymigrationtest"
  region  = "europe-north1"
}

# Buckets
resource "google_storage_bucket" "bqms-migration-demo-data0" {
  name                        = "dzaluzhnyi-bqms-migration-demo-data"
  location                    = "EU"
  force_destroy               = true
  uniform_bucket_level_access = true
}


# Managed folders
resource "google_storage_managed_folder" "crm_leads0" {
  bucket        = "${google_storage_bucket.bqms-migration-demo-data0.name}"
  name          = "crm/leads/"
}

resource "google_storage_managed_folder" "crm_campaigns1" {
  bucket        = "${google_storage_bucket.bqms-migration-demo-data0.name}"
  name          = "crm/campaigns/"
}

resource "google_storage_managed_folder" "erp_accounts_payable2" {
  bucket        = "${google_storage_bucket.bqms-migration-demo-data0.name}"
  name          = "erp/accounts_payable/"
}

resource "google_storage_managed_folder" "erp_accounts_receivable3" {
  bucket        = "${google_storage_bucket.bqms-migration-demo-data0.name}"
  name          = "erp/accounts_receivable/"
}

resource "google_storage_managed_folder" "erp_general_ledger4" {
  bucket        = "${google_storage_bucket.bqms-migration-demo-data0.name}"
  name          = "erp/general_ledger/"
}


# IAM policy bindings
data "google_iam_policy" "bqms-migration-demo-data_crm_leads0" {
  binding {
    role = "roles/storage.objectUser"
    members = [
        "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
    ]
  }
  
}

data "google_iam_policy" "bqms-migration-demo-data_crm_campaigns1" {
  binding {
    role = "roles/storage.objectUser"
    members = [
        "serviceAccount:sales-sa@bqms-migration-demo.iam.gserviceaccount.com",
    ]
  }
  binding {
    role = "roles/storage.objectViewer"
    members = [
        "serviceAccount:marketing-sa@bqms-migration-demo.iam.gserviceaccount.com",
    ]
  }
  
}

data "google_iam_policy" "bqms-migration-demo-data_erp_accounts_payable2" {
  binding {
    role = "roles/storage.objectUser"
    members = [
        "serviceAccount:accounting-sa@bqms-migration-demo.iam.gserviceaccount.com",
    ]
  }
  
}

data "google_iam_policy" "bqms-migration-demo-data_erp_accounts_receivable3" {
  binding {
    role = "roles/storage.objectUser"
    members = [
        "serviceAccount:accounting-sa@bqms-migration-demo.iam.gserviceaccount.com",
    ]
  }
  
}

data "google_iam_policy" "bqms-migration-demo-data_erp_general_ledger4" {
  binding {
    role = "roles/storage.objectUser"
    members = [
        "serviceAccount:accounting-sa@bqms-migration-demo.iam.gserviceaccount.com",
    ]
  }
  
}


# Managed folder IAM policy
resource "google_storage_managed_folder_iam_policy" "bqms-migration-demo-data_crm_leads0_extraloooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong" {
  bucket         = "${google_storage_managed_folder.crm_leads0.bucket}"
  managed_folder = "${google_storage_managed_folder.crm_leads0.name}"
  policy_data    = "${data.google_iam_policy.bqms-migration-demo-data_crm_leads0.policy_data}"
}

resource "google_storage_managed_folder_iam_policy" "bqms-migration-demo-data_crm_campaigns1" {
  bucket         = "${google_storage_managed_folder.crm_campaigns1.bucket}"
  managed_folder = "${google_storage_managed_folder.crm_campaigns1.name}"
  policy_data    = "${data.google_iam_policy.bqms-migration-demo-data_crm_campaigns1.policy_data}"
}

resource "google_storage_managed_folder_iam_policy" "bqms-migration-demo-data_erp_accounts_payable2" {
  bucket         = "${google_storage_managed_folder.erp_accounts_payable2.bucket}"
  managed_folder = "${google_storage_managed_folder.erp_accounts_payable2.name}"
  policy_data    = "${data.google_iam_policy.bqms-migration-demo-data_erp_accounts_payable2.policy_data}"
}

resource "google_storage_managed_folder_iam_policy" "bqms-migration-demo-data_erp_accounts_receivable3" {
  bucket         = "${google_storage_managed_folder.erp_accounts_receivable3.bucket}"
  managed_folder = "${google_storage_managed_folder.erp_accounts_receivable3.name}"
  policy_data    = "${data.google_iam_policy.bqms-migration-demo-data_erp_accounts_receivable3.policy_data}"
}

resource "google_storage_managed_folder_iam_policy" "bqms-migration-demo-data_erp_general_ledger4" {
  bucket         = "${google_storage_managed_folder.erp_general_ledger4.bucket}"
  managed_folder = "${google_storage_managed_folder.erp_general_ledger4.name}"
  policy_data    = "${data.google_iam_policy.bqms-migration-demo-data_erp_general_ledger4.policy_data}"
}