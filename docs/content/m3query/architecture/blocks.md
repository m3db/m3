---
title: "Blocks"
weight: 1
---

**Please note:** This documentation is a work in progress and more detail is required.

## Overview

The fundamental data structures that M3 Query uses are `Blocks`. `Blocks` are what get created from the series iterators that M3DB returns. A `Block` is associated with a start and end time. It contains data from multiple time series stored in columnar format.

Most transformations within M3 Query will be applied across different series for each time interval. Therefore, having data stored in columnar format helps with the memory locality of the data. Moreover, most transformations within M3 Query can work in parallel on different blocks which can significantly increase the computation speed.

## Diagram

Below is a visual representation of a set of `Blocks`. On top is the M3QL query that gets executed, and on the bottom, are the results of the query containing 3 different Blocks.

```
                              ┌───────────────────────────────────────────────────────────────────────┐
                              │                                                                       │
                              │     fetch name:sign_up city_id:{new_york,san_diego,toronto} os:*     │
                              │                                                                       │
                              └───────────────────────────────────────────────────────────────────────┘
                                         │                        │                         │
                                         │                        │                         │
                                         │                        │                         │
                                         ▼                        ▼                         ▼
                                  ┌────────────┐            ┌────────────┐           ┌─────────────┐
                                  │  Block One │            │  Block Two │           │ Block Three │
                                  └────────────┘            └────────────┘           └─────────────┘
                              ┌──────┬──────┬──────┐    ┌──────┬──────┬──────┐   ┌──────┬──────┬──────┐
                              │   t  │ t+1  │ t+2  │    │  t+3 │ t+4  │ t+5  │   │  t+6 │ t+7  │ t+8  │
                              ├──────┼──────┼──────▶    ├──────┼──────┼──────▶   ├──────┼──────┼──────▶
┌───────────────────────────┐ │      │      │      │    │      │      │      │   │      │      │      │
│       name:sign_up        │ │      │      │      │    │      │      │      │   │      │      │      │
│  city_id:new_york os:ios  │ │  5   │  2   │  10  │    │  10  │  2   │  10  │   │  5   │  3   │  5   │
└───────────────────────────┘ │      │      │      │    │      │      │      │   │      │      │      │
                              ├──────┼──────┼──────▶    ├──────┼──────┼──────▶   ├──────┼──────┼──────▶
┌───────────────────────────┐ │      │      │      │    │      │      │      │   │      │      │      │
│       name:sign_up        │ │      │      │      │    │      │      │      │   │      │      │      │
│city_id:new_york os:android│ │  10  │  8   │  5   │    │  20  │  4   │  5   │   │  10  │  8   │  5   │
└───────────────────────────┘ │      │      │      │    │      │      │      │   │      │      │      │
                              ├──────┼──────┼──────▶    ├──────┼──────┼──────▶   ├──────┼──────┼──────▶
┌───────────────────────────┐ │      │      │      │    │      │      │      │   │      │      │      │
│       name:sign_up        │ │      │      │      │    │      │      │      │   │      │      │      │
│ city_id:san_diego os:ios  │ │  10  │  5   │  10  │    │  2   │  5   │  10  │   │  8   │  6   │  6   │
└───────────────────────────┘ │      │      │      │    │      │      │      │   │      │      │      │
                              ├──────┼──────┼──────▶    ├──────┼──────┼──────▶   ├──────┼──────┼──────▶
┌───────────────────────────┐ │      │      │      │    │      │      │      │   │      │      │      │
│       name:sign_up        │ │      │      │      │    │      │      │      │   │      │      │      │
│  city_id:toronto os:ios   │ │  2   │  5   │  10  │    │  2   │  5   │  10  │   │  2   │  5   │  10  │
└───────────────────────────┘ │      │      │      │    │      │      │      │   │      │      │      │
                              └──────┴──────┴──────┘    └──────┴──────┴──────┘   └──────┴──────┴──────┘
```

## M3DB => M3 Query Blocks

In order to convert M3DB blocks into M3 Query blocks, we need to consolidate across different namespaces. In short, M3DB namespaces are essentially different resolutions that metrics are stored at. For example, a metric might be stored at both 1min and 10min resolutions- meaning this metric is found in two namespaces.

At a high level, M3DB returns to M3 Query `SeriesBlocks` that contain a list of `SeriesIterators` for a given timeseries per namespace. M3 Query then aligns the blocks across common time bounds before applying consolidation.

For example, let's say we have a query that returns two timeseries from two different namespaces- 1min and 10min. When we create the M3 Query `Block`, in order to accurately consolidate results from these two namespaces, we need to convert everything to have a 10min resolution. Otherwise it will not be possible to perform correctly apply functions.

> Coming Soon: More documentation on how M3 Query applies consolidation.
