// @flow
import _ from 'lodash';
import dateFns from 'date-fns';

export function getTypeTag(tagString: string): string {
  const regex = /type:([^\s]*)/g;
  const results = regex.exec(tagString);
  return _.nth(results, 1) || null;
}

export function filterPoliciesBasedOnTag(
  policies: Array<string>,
  typeTag: string,
) {
  if (_.includes(['gauge', 'counter'], typeTag)) {
    return policies;
  }
  return _.filter(policies, p => !p.startsWith('10m'));
}

export function formatTimestampMilliseconds(timestamp: number): string {
  if (!timestamp) {
    return 'N/A';
  }
  return dateFns.format(timestamp, 'MM-DD-YYYY hh:mm:ss a');
}
