SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/../../.ci/common.mk

gopath_prefix        := $(GOPATH)/src
m3db_package         := github.com/m3db/m3
m3db_package_path    := $(gopath_prefix)/$(m3db_package)

# Generation rule for all generated types
.PHONY: genny-all
genny-all: genny-aggregator-counter-elem genny-aggregator-timer-elem genny-aggregator-gauge-elem

.PHONY: genny-aggregator-counter-elem
genny-aggregator-counter-elem:
	cat $(m3db_package_path)/src/aggregator/aggregator/generic_elem.go                                    \
		| awk '/^package/{i++}i'                                                                            \
		| sed 's/metric.GaugeType/metric.CounterType/'														\
		| genny -out=$(m3db_package_path)/src/aggregator/aggregator/counter_elem_gen.go -pkg=aggregator gen \
		"timedAggregation=timedCounter lockedAggregation=lockedCounterAggregation typeSpecificAggregation=counterAggregation typeSpecificElemBase=counterElemBase genericElemPool=CounterElemPool GenericElem=CounterElem"

.PHONY: genny-aggregator-timer-elem
genny-aggregator-timer-elem:
	cat $(m3db_package_path)/src/aggregator/aggregator/generic_elem.go                                  \
		| awk '/^package/{i++}i'                                                                          \
		| genny -out=$(m3db_package_path)/src/aggregator/aggregator/timer_elem_gen.go -pkg=aggregator gen \
		"timedAggregation=timedTimer lockedAggregation=lockedTimerAggregation typeSpecificAggregation=timerAggregation typeSpecificElemBase=timerElemBase genericElemPool=TimerElemPool GenericElem=TimerElem"

.PHONY: genny-aggregator-gauge-elem
genny-aggregator-gauge-elem:
	cat $(m3db_package_path)/src/aggregator/aggregator/generic_elem.go                                  \
		| awk '/^package/{i++}i'                                                                          \
		| genny -out=$(m3db_package_path)/src/aggregator/aggregator/gauge_elem_gen.go -pkg=aggregator gen \
		"timedAggregation=timedGauge lockedAggregation=lockedGaugeAggregation typeSpecificAggregation=gaugeAggregation typeSpecificElemBase=gaugeElemBase genericElemPool=GaugeElemPool GenericElem=GaugeElem"
