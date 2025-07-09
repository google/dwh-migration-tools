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
  project = "fix the project name"
  region  = "fix the region"
}

# Buckets
{% for resource_name, bucket in resource.google_storage_bucket.items() -%}
resource "google_storage_bucket" "{{resource_name}}" {
  name                        = "{{bucket.name}}"
  location                    = "{{bucket.location}}"
  force_destroy               = true
  uniform_bucket_level_access = true
}
{%- endfor %}

# Managed folders
{%- for resource_name, folder in resource.google_storage_managed_folder.items() %}
resource "google_storage_managed_folder" "{{resource_name}}" {
  bucket        = "{{folder.bucket}}"
  name          = "{{folder.name}}"
}
{%- endfor %}

# IAM policy bindings{% for resource_name, policy in data.google_iam_policy.items() %}
data "google_iam_policy" "{{resource_name}}" {
  {%- for role_binding in policy.binding %}
  binding {
    role = "{{role_binding.role}}"
    members = [
        {% for member in role_binding.members %}"{{member}}",{% endfor %}
    ]
  }
  {%- endfor %}
}
{% endfor %}

# Managed folder IAM policy
{%- for resource_name, policy in resource.google_storage_managed_folder_iam_policy.items() %}
resource "google_storage_managed_folder_iam_policy" "{{resource_name}}" {
  bucket         = "{{policy.bucket}}"
  managed_folder = "{{policy.managed_folder}}"
  policy_data    = "{{policy.policy_data}}"
}
{% endfor %}