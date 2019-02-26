import {getTypeTag, filterPoliciesBasedOnTag} from './index';

describe('getTypeTag()', () => {
  it('should parse out the type tag from a metric filter', () => {
    expect(getTypeTag('type:timer')).toBe('timer');
    expect(getTypeTag('type:a')).toBe('a');
    expect(getTypeTag('this:test type')).toBe(null);
    expect(getTypeTag('type:counts ')).toBe('counts');
  });
});

describe('filterPoliciesBasedOnTag()', () => {
  it('should not remove any policies for gauges', () => {
    expect(
      filterPoliciesBasedOnTag(['1m:10s', '10m:10s', '10m:30d'], 'gauge'),
    ).toEqual(['1m:10s', '10m:10s', '10m:30d']);
  });

  it('should not remove any policies for counters', () => {
    expect(
      filterPoliciesBasedOnTag(['1m:10s', '10m:10s', '10m:30d'], 'counter'),
    ).toEqual(['1m:10s', '10m:10s', '10m:30d']);
  });

  it('should remove any policies over 1m resolution when not a gauge or counter', () => {
    const input = ['1m:10s', '10m:10s', '10m:30d'];
    const output = ['1m:10s'];
    const typesToTest = ['', null, 'timers'];

    typesToTest.forEach(type =>
      expect(filterPoliciesBasedOnTag(input, type)).toEqual(output),
    );
  });
});
