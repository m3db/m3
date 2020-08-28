---
title: "Operator"
date: 2020-05-08T12:43:53-04:00
draft: true
---

Introduction
Welcome to the documentation for the M3DB operator, a Kubernetes operator for running the open-source timeseries database M3DB on Kubernetes.

Please note that this is alpha software, and as such its APIs and behavior are subject to breaking changes. While we aim to produce thoroughly tested reliable software there may be undiscovered bugs.

For more background on the M3DB operator, see our KubeCon keynote on its origins and usage at Uber.

Philosophy
The M3DB operator aims to automate everyday tasks around managing M3DB. Specifically, it aims to automate:

Creating M3DB clusters
Destroying M3DB clusters
Expanding clusters (adding instances)
Shrinking clusters (removing instances)
Replacing failed instances
It explicitly does not try to automate every single edge case a user may ever run into. For example, it does not aim to automate disaster recovery if an entire cluster is taken down. Such use cases may still require human intervention, but the operator will aim to not conflict with such operations a human may have to take on a cluster.

Generally speaking, the operator's philosophy is if it would be unclear to a human what action to take, we will not try to guess.