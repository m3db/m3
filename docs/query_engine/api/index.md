# API

**Please note:** This documentation is a work in progress and more detail is required.

**Read using prometheus query**
----
  Returns datapoints in Grafana format based on the PromQL expression.

* **URL**

  /prom/native/read

* **Method:**

  `GET`

*  **URL Params**

   **Required:**

   `start=[time in RFC3339Nano]`
   `end=[time in RFC3339Nano]`
   `step=[time duration]`
   `target=[string]`

   **Optional:**
   `debug=[bool]`

* **Data Params**

  None

* **Success Response:**

  * **Code:** 200 <br />

* **Error Response:**

* **Sample Call:**

  ```
  curl 'http://localhost:9090/api/v1/query_range?query=abs(http_requests_total)&start=1530220860&end=1530220900&step=15s'
  {
    "status": "success",
    "data": {
      "resultType": "matrix",
      "result": [
        {
          "metric": {
            "code": "200",
            "handler": "graph",
            "instance": "localhost:9090",
            "job": "prometheus",
            "method": "get"
          },
          "values": [
            [
              1530220860,
              "6"
            ],
            [
              1530220875,
              "6"
            ],
            [
              1530220890,
              "6"
            ]
          ]
        },
        {
          "metric": {
            "code": "200",
            "handler": "label_values",
            "instance": "localhost:9090",
            "job": "prometheus",
            "method": "get"
          },
          "values": [
            [
              1530220860,
              "6"
            ],
            [
              1530220875,
              "6"
            ],
            [
              1530220890,
              "6"
            ]
          ]
        }
      ]
    }
  }
  ```