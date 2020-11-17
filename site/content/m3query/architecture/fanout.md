---
title: "Fetching and querying"
menuTitle: "Query Fanout"
weight: 2
---

## Fetch fanout

Since m3query does not currently have a view into the M3DB index, fanout to multiple clusters is rather complicated. Since not every metric is necessarily in every cluster (as an example, carbon metrics routed to a certain resolution), it is not trivial to determine which namespaces should be queried to return a fully correct set of recorded metrics.

The general approach is therefore to attempt to fanout to any namespace which has a complete view of all metrics, for example, `Unaggregated`, and take that if it fulfills the query range; if not, m3query will attempt to stitch together namespaces with longer retentions to try and build the most complete possible view of stored metrics.