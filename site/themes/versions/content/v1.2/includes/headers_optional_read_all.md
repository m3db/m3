* `M3-Restrict-By-Tags-JSON`:  
 If this header is set it can ensure specific label matching is performed as part
of every query including series metadata endpoints. As an example, the following 
header would unconditionally cause `globaltag=somevalue` to be a part of all queries
issued regardless of if they include the label or not in a query and also strip the
"globaltag" from appearing as a label in any of the resulting timeseries:
```
M3-Restrict-By-Tags-JSON: '{"match":[{"name":"globaltag","type":"EQUAL","value":"somevalue"}],"strip":["globaltag"]}'
```

{{% fileinclude "docs/includes/headers_optional_read_limits.md" %}}