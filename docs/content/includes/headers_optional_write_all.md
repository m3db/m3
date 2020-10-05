* `M3-Map-Tags-JSON`:  
 If this header is set it enables dynamically mutating tags in a Prometheus write request. See issue
[2254](https://github.com/m3db/m3/issues/2254) for further context.
Currently only `write` is supported. As an example, the following header would unconditionally cause
`globaltag=somevalue` to be added to all metrics in a write request:
```
M3-Map-Tags-JSON: '{"tagMappers":[{"write":{"tag":"globaltag","value":"somevalue"}}]}'
```