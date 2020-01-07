provider "google" {
  project     = "${var.gcp_project_id}"
  region      = "${var.gcp_region}"
}

resource "google_container_cluster" "primary" {
  name               = "m3db-bundle-cluster"
  location           = "${var.gcp_region}"
  initial_node_count = 3

  
  # Setting an empty username and password explicitly disables basic auth
  master_auth {
    username = ""
    password = ""
  }

  node_config {
    preemptible  = false
    machine_type = "${var.gcp_instance_type}"

    oauth_scopes = [
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
    ]

    metadata {
      disable-legacy-endpoints = "true"
    }
  }

  timeouts {
    create = "30m"
    update = "40m"
  }
}

provider "kubernetes" {
  host = "${google_container_cluster.primary.endpoint}"

  client_certificate     = "${base64decode(google_container_cluster.primary.master_auth.0.client_certificate)}"
  client_key             = "${base64decode(google_container_cluster.primary.master_auth.0.client_key)}"
  cluster_ca_certificate = "${base64decode(google_container_cluster.primary.master_auth.0.cluster_ca_certificate)}"
}
