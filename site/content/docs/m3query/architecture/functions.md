---
title: "Function Processing"
weight: 3
---

## Supported Functions

| M3QL                                                   | Prometheus                 | Graphite                                                             |
| ------------------------------------------------------ | -------------------------- | -------------------------------------------------------------------- |
| abs/absolute                                           | abs()                      | absolute(seriesList)                                                 |
| alias [alias]                                          |                            | alias(seriesList, newName)                                           |
| aliasByTags [tag]                                      |                            | aliasByTags(seriesList, \*tags)                                      |
| aliasByBucket/aliasByHistogramBucket [tag]             |                            |                                                                      |
| anomalies [flags]                                      |                            |                                                                      |
| asPercent                                              | /                          | asPercent(seriesList, total=None, \*nodes)                           |
| avg/averageSeries [tag]                                | avg()                      | averageSeries(\*seriesLists)                                         |
| changed                                                |                            | changed(seriesList)                                                  |
| constantLine [value]                                   |                            | constantLine(value)                                                  |
| count                                                  | count()                    | countSeries(\*seriesLists)                                           |
| derivative                                             |                            | derivative(seriesList)                                               |
| diff                                                   | -                          | diffSeries(\*seriesLists)                                            |
| divideSeries                                           | /                          | divideSeries(dividendSeriesList, divisorSeries)                      |
| eq/== [value]                                          | ==                         | removeBelowValue(seriesList, n)/removeAboveValue(seriesList, n)      |
| ne/!= [value]                                          | !=                         | removeBelowValue(seriesList, n)/removeAboveValue(seriesList, n)      |
| excludeByTag [tag, pattern]                            |                            | exclude(seriesList, pattern)                                         |
| execute/exec [fetch]                                   |                            |                                                                      |
| fallbackSeries [replacement]                           |                            | fallbackSeries(seriesList, fallback)                                 |
| fetch                                                  |                            |                                                                      |
| ge/=> [value]                                          | >=                         | removeBelowValue(seriesList, n)                                      |
| gt/> [value]                                           | >                          | removeBelowValue(seriesList, n)                                      |
| head [limit]                                           | topk()                     | highest(seriesList, n=1, func='average')                             |
| histogramCDF [idTag, rangeTag, value]                  |                            |                                                                      |
| histogramPercentile [idTag, rangeTag, percentileValue] |                            |                                                                      |
| identity [name]                                        |                            | identity(name)                                                       |
| integral                                               |                            | integral(seriesList)                                                 |
| intersect [tags]                                       | and/or                     |                                                                      |
| isNonNull                                              |                            | isNonNull(seriesList)                                                |
| jainCP                                                 |                            |                                                                      |
| keepLastValue                                          |                            | keepLastValue(seriesList, limit=inf)                                 |
| le/&lt;= [value]                                       | &lt;=                      | removeAboveValue(seriesList, n)                                      |
| logarithm                                              | ln()                       | logarithm(seriesList, base=10)                                       |
| lt/&lt; [value]                                        | &lt;                       | removeAboveValue(seriesList, n)                                      |
| max/maxSeries [tag]                                    | max()                      | maxSeries(\*seriesLists)                                             |
| min/minSeries [tag]                                    | min()                      | minSeries(\*seriesLists)                                             |
| moving [interval, func]                                | <aggregation>\_over_time() | movingMax, movingMin, movingMedian, movingAverage, etc.              |
| multiply/multiplySeries [tag]                          | \*                         | multiplySeries(\*seriesLists)                                        |
| nonNegativeDerivative [maxValue]                       |                            | nonNegativeDerivative(seriesList, maxValue=None)                     |
| nPercentile [percentile]                               |                            | nPercentile(seriesList, n)                                           |
| offset [amount]                                        |                            | offset(seriesList, factor)                                           |
| percentileOfSeries [n, true/false, tag]                |                            | percentileOfSeries(seriesList, n, interpolate=False)                 |
| perSecond                                              | rate()                     | perSecond(seriesList, maxValue=None)                                 |
| promHistogramPercentile [percentileValue]              |                            |                                                                      |
| range [tag]                                            |                            | rangeOfSeries(\*seriesLists)                                         |
| removeAbovePercentile [percentile]                     |                            | removeAbovePercentile(seriesList, n)                                 |
| removeBelowPercentile [percentile]                     |                            | removeBelowPercentile(seriesList, n)                                 |
| removeAboveValue [value]                               |                            | removeAboveValue(seriesList, n)                                      |
| removeBelowValue [value]                               |                            | removeBelowValue(seriesList, n)                                      |
| removeEmpty                                            |                            | removeEmptySeries(seriesList, xFilesFactor=None)                     |
| scale [factor]                                         |                            | scale(seriesList, factor)                                            |
| scaleToSeconds [seconds]                               |                            | scaleToSeconds(seriesList, seconds)                                  |
| setDiff [tags]                                         |                            |                                                                      |
| showAnomalyThresholds [level, model]                   |                            |                                                                      |
| showTags [true/false, tagName(s)]                      |                            |                                                                      |
| sort/sortSeries [avg, current, max, stddev, sum]       | sort()                     | sortBy(seriesList, func='average', reverse=False)                    |
| stdev [points, windowTolerance]                        | stddev()                   | stdev(seriesList, points, windowTolerance=0.1)                       |
| sqrt/squareRoot                                        | sqrt()                     | squareRoot(seriesList)                                               |
| summarize [interval, func, alignToFrom]                |                            | summarize(seriesList, intervalString, func='sum', alignToFrom=False) |
| sum/sumSeries [tag]                                    | sum()                      | sumSeries(\*seriesLists)                                             |
| sustain [duration]                                     |                            |                                                                      |
| sustainedAbove & sustainedBelow                        |                            |                                                                      |
| tail [limit]                                           | bottomk()                  | lowest(seriesList, n=1, func='average')                              |
| timeshift [duration]                                   |                            | timeShift(seriesList, timeShift, resetEnd=True, alignDST=False)      |
| timestamp                                              | timestamp()                |                                                                      |
| transformNull [value]                                  |                            | transformNull(seriesList, default=0, referenceSeries=None)           |
