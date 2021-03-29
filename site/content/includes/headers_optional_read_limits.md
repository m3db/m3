* `M3-Limit-Max-Series`:  
 If this header is set it will override any configured per query time series limit. If the limit is hit, it will either return a partial result or an error based on the require exhaustive configuration set.<br />
* `M3-Limit-Max-Docs`:  
 If this header is set it will override any configured per query time series * blocks limit (docs limit). If the limit is hit, it will either return a partial result or an error based on the require exhaustive configuration set.<br />
* `M3-Limit-Require-Exhaustive`:  
 If this header is set it will override any configured require exhaustive setting. If "true" it will return an error if query hits a configured limit (such as series or docs limit) instead of a partial result. Otherwise if "false" it will return a partial result of the time series already matched with the response header `M3-Results-Limited` detailing the limit that was hit and a warning included in the response body.