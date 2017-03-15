/**
* Different types of values that m3 emits. Each metric
* must contain one of these values
*/
union MetricValue {
	1: optional CountValue	count
	2: optional GaugeValue	gauge
	3: optional TimerValue	timer
}

/**
* Different types of count values
*/
union CountValue {
	1: optional i64	i64Value
}

/**
* Different types of gauge values
*/
union GaugeValue {
	1: optional i64		i64Value
	2: optional double	dValue
}

/**
* Different types of timer values
*/
union TimerValue {
	1: optional i64		i64Value
	2: optional double	dValue
}

/**
* Tags that can be applied to a metric
*/
struct MetricTag {
	1: string		tagName,
	2: optional string	tagValue
}

/**
* The metric that is being emitted
*/
struct Metric {
	1: string			name,
	2: optional MetricValue		metricValue,
	3: optional i64			timestamp,
	4: optional set<MetricTag>	tags
}

/**
* Structure that holds a group of metrics which share
* common properties like the cluster and service.
*/
struct MetricBatch {
	1: list<Metric>			metrics
	2: optional set<MetricTag>	commonTags
}

/**
* M3 Metrics Service
*/
service M3 {

	/**
	* Emits a batch of metrics.
	*/
	oneway void emitMetricBatch(1: MetricBatch batch)
}
