---
title: "M3DB Operator"
menuTitle: "M3DB Operator"
weight: 1
chapter: true
---

This documentation is for the M3DB [Kubernetes operator][operators] which can run and operate [M3DB][m3db] on Kubernetes.

For more background on the M3DB operator, see our [KubeCon keynote][keynote] on its origins and usage at Uber.

## Philosophy

The M3DB operator aims to automate everyday tasks around managing M3DB. Specifically, it aims to automate:

- Creating M3DB clusters
- Destroying M3DB clusters
- Expanding clusters (adding instances)
- Shrinking clusters (removing instances)
- Replacing failed instances

It explicitly does not try to automate every single edge case a user may ever run into. For example, it does not aim to
automate disaster recovery if an entire cluster is taken down. Such use cases may still require human intervention, but
the operator will aim to not conflict with such operations a human may have to take on a cluster.

Generally speaking, the operator's philosophy is if **it would be unclear to a human what action to take, we will not
try to guess.**

[operators]: https://coreos.com/operators/
[m3db]: https://m3db.io/docs/
[keynote]: https://kccna18.sched.com/event/Gsxn/keynote-smooth-operator-large-scale-automated-storage-with-kubernetes-celina-ward-software-engineer-matt-schallert-site-reliability-engineer-uber
