const helpText = {
  namespace:
    'Logical grouping of rules. The namespace name must be a valid service name.',
  'mapping-rule':
    'Configure the resolution, retention, and optional custom aggregation functions for metrics matching the filter.',
  'rollup-rule':
    'Configure how to roll up metrics matching the filter and the corresponding policies. A matching metric triggers the generation of a new rollup metric whose name, tags, and policies are defined by the rollup targets. ',
  'metric-filter':
    ' A list of tag name:glob pattern pairs identifying matching metrics.',
  policy:
    'Defines the resolution, retention period, and optional custom aggregation functions for given metric.',
  target:
    'Defines how a new rollup metric is generated and its policies. The rollup metric will use “Rollup Metric Name” as its name, “Rollup Tags” as its tags, and “Policies” as its policies.  ',
  'rollup-tag': 'Tags retained by the generated rollup metric.',
  'resolution:retention-period':
    'Describes the resolution at which the metric will be aggregated, and how long the metric will be stored for. ',
  'aggregation-function':
    'Defines how the given metric is aggregated. For example, an aggregation function of P99 returns the 99th percentile of the given metric.',
  'effective-time': 'The time at which the rule will be in effect',
};

export function getHelpText(helpTextKey) {
  if (!helpText[helpTextKey]) {
    console.error(`The helpKey: ${helpTextKey} does not exist`); // eslint-disable-line
  }
  return helpText[helpTextKey] || '';
}
