---
title: "Install"
date: 2020-05-08T12:46:04-04:00
draft: true
---

Installation
Be sure to take a look at the requirements before installing the operator.

Helm
Add the m3db-operator repo:
helm repo add m3db https://m3-helm-charts.storage.googleapis.com/stable
Install the m3db-operator chart:
helm install m3db/m3db-operator --namespace m3db-operator
Note: If uninstalling an instance of the operator that was installed with Helm, some resources such as the ClusterRole, ClusterRoleBinding, and ServiceAccount may need to be deleted manually.

Manually
Install the bundled operator manifests in the current namespace:

kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/bundle.yaml 
