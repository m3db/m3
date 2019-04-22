# GCP M3DB Bundle Kubernetes Example

This folder shows an example of how to launch the M3DB Kubernetes Bundle on GKE. It sets up a GKE Kube Cluster with three n1-standard-16 nodes. It also creates the GCP specific fast Kubernetes storage class. If using the default terraform created GCP user 'client', the following yaml will need to be applied via kubectl before executing the main bundle tf file:

```yaml
ClusterRoleBinding 
apiVersion: rbac.authorization.k8s.io/v1 
metadata: 
name: client
subjects: 
- kind: User 
name: client
roleRef: 
kind: ClusterRole 
name: "cluster-admin" 
apiGroup: rbac.authorization.k8s.io

```
