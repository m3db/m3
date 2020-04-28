# Query comparator

This docker-compose file will setup the following environment:

1. 1 M3Comparator node that acts as remote gRPC storage. Provides randomized data based on the incoming query's start time.
2. 1 M3Query node that connects to the M3Comparator instance, using it as remote storage. Serves queries and remote reads.
3. 1 Prometheus node that has no scrape settings set, connecting to M3Query instance as a remote_read endpoint.
4. (optionally) 1 Grafana node with pre-configured graphs corresponding to the queries run by the test.

## Mechanism

- Queries are generated from `queries.json`, then run against both the Prometheus and M3Query instances, then results are compared.

## Usage

- Use `make docker-compatibility-test` from the base folder to run the comparator tests.
- Use `CI=FALSE make docker-compatibility-test` from the base folder to run the comparator tests, brings up a Grafana instance and does not perform teardown, allowing manual inspection of query differences.

## Grafana

Use Grafana by navigating to `http://localhost:3000` and using `admin` for both the username and password. The dashboard should already be populated and working, it should be named `Dashboard <git-reference>`.

## Comparator tests diagram

```
                    ┌───────────────────┐                            
                    │                   │                            
                    │ m3comparator      │                            
                    │                   │                            
                    │   1. random data  │                            
                    │   2. loaded data  │                            
                    │                   │ ┌───────────────┐          
                    └───────────────────┘ │   raw data    │          
                              ▲           │ (GRPC M3Query │          
                              │           │Remote Storage)│          
                              │           └───────────────┘          
                              └──────────────────────────┐           
                                                         │           
                                                         │           
                                                         │           
┌───────────────────────┐                     ┌─────────────────────┐
│                       │                     │                     │
│                       │                     │                     │
│                       │                     │                     │
│     prometheus        │────────────────────▶│      m3query        │
│                       │  ┌────────────┐     │                     │
│                       │  │  raw data  │     │                     │
│                       │  │(Prom Remote│     │                     │
└───────────────────────┘  │   Read)    │     └─────────────────────┘
            ▲              └────────────┘                ▲           
            │                                            │           
            │                                            │           
            │                                            │           
            └────────────────────┬───────────────────────┘           
    ┌───────────┐                │                ┌───────────┐      
    │   query   │                │                │   query   │      
    │ (PromQL)  │                │                │ (PromQL)  │      
    └───────────┘                │                └───────────┘      
                          ┌────────────┐                             
                          │            │                             
                          │ compare.go │                             
                          │            │                             
                          └────────────┘                             
```
