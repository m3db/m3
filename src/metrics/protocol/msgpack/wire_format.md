## Wire format for unaggregated metrics

* Message format
  * Version
  * Number of root object fields
  * Root object type
  * Root object (can be one of the following):
    * CounterWithPolicies
    * BatchTimerWithPolicies
    * GaugeWithPolicies

* CounterWithPolicies object
  * Number of CounterWithPolicies fields
  * Counter object
  * VersionedPolicies object

* BatchTimerWithPolicies object
  * Number of BatchTimerWithPolicies fields
  * BatchTimer object
  * VersionedPolicies object

* GaugeWithPolicies object
  * Number of GaugeWithPolicies fields
  * Gauge object
  * VersionedPolicies object

* Counter object
  * Number of Counter fields
  * Counter ID
  * Counter value

* BatchTimer object
  * Number of BatchTimer fields
  * BatchTimer ID
  * BatchTimer values

* Gauge object
  * Number of Gauge fields
  * Gauge ID
  * Gauge value

* VersionedPolicies object
  * Number of VersionedPolicies fields
  * Versioned Policies (can be one of the following)
    * DefaultVersionedPolicies
      * Versioned Policies type
    * CustomVersionedPolicies
      * Versioned Policies type
      * Version
      * Cutover
      * List of Policy objects

* Policy object
  * Number of Policy fields
  * Resolution object
  * Retention object

* Resolution object
  * Number of Resolution fields
  * Resolution (can be one of the following)
    * KnownResolution
      * Resolution type
      * ResolutionValue
    * UnknownResolution
      * Resolution type
      * Resolution window in nanoseconds
      * Resolution precision

* Retention object
  * Number of Retention fields
  * Retention (can be one of the following)
    * KnownRetention
      * Retention type
      * RetentionValue
    * UnknownRetention
      * Retention type
      * Retention duration in nanoseconds

## Wire format for aggregated metrics

* Message format
  * Version
  * Number of root object fields
  * Root object type
  * Root object (can be one of the following):
    * RawMetricWithPolicy

* RawMetricWithPolicy object
  * Number of RawMetricWithPolicy fields
  * Raw metric object
  * Policy object

* Raw metric object
  * Version
  * Number of metric fields
  * Metric ID
  * Metric timestamp
  * Metric value

* Policy object (same format as in unaggregated metrics)

## Schema changes

Backward-compatible changes (e.g., adding an additional field to the end of an object) can be
deployed or rolled back separately on the client-side and the server-side. It is unnecessary to
increase the version for backward-compatible changes.

Backward-incompatible changes (e.g., removing a field or changing a field type) must be deployed
to the server-side first then to the client-side. It is REQUIRED to increase the version for
backward-incompatible changes. If the changes are deployed to the client-side first, the server
will optionally ignore the messages with the higher version.