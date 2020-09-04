---
title: "Managing M3 Query"
date: 2020-04-21T20:59:17-04:00
draft: true
---

### M3DB => M3 Query Blocks

In order to convert M3DB blocks into M3 Query blocks, we need to consolidate across different namespaces. In short, M3DB namespaces are essentially different resolutions that metrics are stored at. For example, a metric might be stored at both 1min and 10min resolutions- meaning this metric is found in two namespaces.

At a high level, M3DB returns to M3 Query SeriesBlocks that contain a list of SeriesIterators for a given timeseries per namespace. M3 Query then aligns the blocks across common time bounds before applying consolidation.

For example, let's say we have a query that returns two timeseries from two different namespaces- 1min and 10min. When we create the M3 Query Block, in order to accurately consolidate results from these two namespaces, we need to convert everything to have a 10min resolution. Otherwise it will not be possible to perform correctly apply functions.

Coming Soon: More documentation on how M3 Query applies consolidation.

### Fetching and querying
Fetch fanout
Since m3query does not currently have a view into the M3DB index, fanout to multiple clusters is rather complicated. Since not every metric is necessarily in every cluster (as an example, carbon metrics routed to a certain resolution), it is not trivial to determine which namespaces should be queried to return a fully correct set of recorded metrics.
The general approach is therefore to attempt to fanout to any namespace which has a complete view of all metrics, for example, Unaggregated, and take that if it fulfills the query range; if not, m3query will attempt to stitch together namespaces with longer retentions to try and build the most complete possible view of stored metrics.
For further details, please ask questions on our gitter, and we'll be happy to help!

Function Processing
Supported Functions
M3QL
Prometheus
Graphite
abs/absolute
abs()
absolute(seriesList)
alias [alias]


alias(seriesList, newName)
aliasByTags [tag]


aliasByTags(seriesList, *tags)
aliasByBucket/aliasByHistogramBucket [tag]




anomalies [flags]




asPercent
/
asPercent(seriesList, total=None, *nodes)
avg/averageSeries [tag]
avg()
averageSeries(*seriesLists)
changed


changed(seriesList)
constantLine [value]


constantLine(value)
count
count()
countSeries(*seriesLists)
derivative


derivative(seriesList)
diff
-
diffSeries(*seriesLists)
divideSeries
/
divideSeries(dividendSeriesList, divisorSeries)
eq/== [value]
==
removeBelowValue(seriesList, n)/removeAboveValue(seriesList, n)
ne/!= [value]
!=
removeBelowValue(seriesList, n)/removeAboveValue(seriesList, n)
excludeByTag [tag, pattern]


exclude(seriesList, pattern)
execute/exec [fetch]




fallbackSeries [replacement]


fallbackSeries(seriesList, fallback)
fetch




ge/=> [value]
>=
removeBelowValue(seriesList, n)
gt/> [value]
>
removeBelowValue(seriesList, n)
head [limit]
topk()
highest(seriesList, n=1, func='average')
histogramCDF [idTag, rangeTag, value]




histogramPercentile [idTag, rangeTag, percentileValue]




identity [name]


identity(name)
integral


integral(seriesList)
intersect [tags]
and/or


isNonNull


isNonNull(seriesList)
jainCP




keepLastValue


keepLastValue(seriesList, limit=inf)
le/<= [value]
<=
removeAboveValue(seriesList, n)
logarithm
ln()
logarithm(seriesList, base=10)
lt/< [value]
<
removeAboveValue(seriesList, n)
max/maxSeries [tag]
max()
maxSeries(*seriesLists)
min/minSeries [tag]
min()
minSeries(*seriesLists)
moving [interval, func]
_over_time()
movingMax, movingMin, movingMedian, movingAverage, etc.
multiply/multiplySeries [tag]
*
multiplySeries(*seriesLists)
nonNegativeDerivative [maxValue]


nonNegativeDerivative(seriesList, maxValue=None)
nPercentile [percentile]


nPercentile(seriesList, n)
offset [amount]


offset(seriesList, factor)
percentileOfSeries [n, true/false, tag]


percentileOfSeries(seriesList, n, interpolate=False)
perSecond
rate()
perSecond(seriesList, maxValue=None)
promHistogramPercentile [percentileValue]




range [tag]


rangeOfSeries(*seriesLists)
removeAbovePercentile [percentile]


removeAbovePercentile(seriesList, n)
removeBelowPercentile [percentile]


removeBelowPercentile(seriesList, n)
removeAboveValue [value]


removeAboveValue(seriesList, n)
removeBelowValue [value]


removeBelowValue(seriesList, n)
removeEmpty


removeEmptySeries(seriesList, xFilesFactor=None)
scale [factor]


scale(seriesList, factor)
scaleToSeconds [seconds]


scaleToSeconds(seriesList, seconds)
setDiff [tags]




showAnomalyThresholds [level, model]




showTags [true/false, tagName(s)]




sort/sortSeries [avg, current, max, stddev, sum]
sort()
sortBy(seriesList, func='average', reverse=False)
stdev [points, windowTolerance]
stddev()
stdev(seriesList, points, windowTolerance=0.1)
sqrt/squareRoot
sqrt()
squareRoot(seriesList)
summarize [interval, func, alignToFrom]


summarize(seriesList, intervalString, func='sum', alignToFrom=False)
sum/sumSeries [tag]
sum()
sumSeries(*seriesLists)
sustain [duration]




sustainedAbove & sustainedBelow




tail [limit]
bottomk()
lowest(seriesList, n=1, func='average')
timeshift [duration]


timeShift(seriesList, timeShift, resetEnd=True, alignDST=False)
timestamp
timestamp()


transformNull [value]


transformNull(seriesList, default=0, referenceSeries=None)




### Setting up m3query
#### Introduction
m3query is used to query data that is stored in M3DB. For instance, if you are using the Prometheus remote write endpoint with m3coordinator, you can use m3query instead of the Prometheus remote read endpoint. By doing so, you get all of the benefits of m3query's engine such as block processing. Furthermore, since m3query provides a Prometheus compatible API, you can use 3rd party graphing and alerting solutions like Grafana.

#### Configuration
Before setting up m3query, make sure that you have at least one M3DB node running. In order to start m3query, you need to configure a yaml file, that will be used to connect to M3DB. Here is a link to a sample config file that is used for an embedded etcd cluster within M3DB.

#### Running
You can run m3query by either building and running the binary yourself:
make m3query
./bin/m3query -f ./src/query/config/m3query-local-etcd.yml

Or you can run it with Docker using the Docker file located at $GOPATH/src/github.com/m3db/m3/docker/m3query/Dockerfile.
