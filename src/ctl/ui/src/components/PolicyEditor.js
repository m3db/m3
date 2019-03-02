// @flow

// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import React from 'react';
import {Select, Icon, Button} from 'antd';
import _ from 'lodash';

import HelpTooltip from './HelpTooltip';

export const AGGREGATION_FUNCTIONS = [
  'Min',
  'Max',
  'Mean',
  'Median',
  'Count',
  'Sum',
  'SumSq',
  'Stdev',
  'P10',
  'P20',
  'P30',
  'P40',
  'P50',
  'P60',
  'P70',
  'P80',
  'P90',
  'P95',
  'P99',
  'P999',
  'P9999',
];

export const POLICIES = [
  '10s:2d',
  '1m:2d',
  '1m:40d',
  '10m:180d',
  '10m:1y',
  '10m:3y',
  '10m:5y',
  '1h:1y',
  '1h:3y',
  '1h:5y',
];

const Option = Select.Option;

type Props = {
  value: any,
  onChange: e => any,
  showDelete: boolean,
  onDeleteClicked: () => any,
  typeTag: string,
};

type PolicyObject = {
  timePolicy: string,
  aggFunctions: array<string>,
};

export function parsePolicy(policy: string): PolicyObject {
  const policyParsed = policy.split('|');
  return {
    timePolicy: _.first(policyParsed),
    aggFunctions: _.chain(policyParsed)
      .nth(1)
      .thru(value => {
        if (_.isEmpty(value)) {
          return [];
        }
        return value.split(',');
      })
      .value(),
  };
}

export function stringifyPolicy(policy: PolicyObject): string {
  return `${policy.timePolicy}${_.isEmpty(policy.aggFunctions)
    ? ''
    : `|${policy.aggFunctions.join(',')}`}`;
}

export function PolicyEditor(props: Props) {
  const {
    value = _.first(POLICIES),
    onDeleteClicked = noop => noop,
    showDelete = false,
    onChange = noop => noop,
    typeTag = '',
    policyList = POLICIES,
  } = props;
  const policy = parsePolicy(value);
  const handleChange = (field, newValue) => {
    onChange(
      stringifyPolicy({
        ...policy,
        [field]: newValue,
      }),
    );
  };
  const showAggs = typeTag === 'timer';
  let policies = policyList;
  if (!_.includes(['gauge', 'counter'], typeTag)) {
    policies = policies.filter(p => !p.startsWith('1h'));
  }

  return (
    <div className="clearfix p1 relative" style={{}}>
      {showDelete && (
        <a
          className="absolute"
          style={{right: 10, top: 10, zIndex: 100}}
          onClick={onDeleteClicked}>
          <Icon type="close-circle-o" />
        </a>
      )}
      <div className="col col-6 px1">
        <label className="block pb1">
          Resolution:Retention Period{' '}
          <HelpTooltip helpTextKey="resolution:retention-period" />
        </label>
        <Select
          placeholder="Select a policy"
          value={policy.timePolicy}
          autoComplete="off"
          onChange={e => {
            handleChange('timePolicy', e);
          }}
          style={{width: '100%'}}
          notFoundContent={false}>
          {policies.map(opt => <Option key={opt}>{opt}</Option>)}
        </Select>
        {typeTag === 'timer' && (
          <small style={{color: 'gray'}}>
            Resolutions {'>'} 1 minute are not supported for timers
          </small>
        )}
      </div>
      <div className="col col-6" style={{opacity: showAggs ? 1 : 0.5}}>
        <label className="block pb1">
          Aggregation Functions{' '}
          <HelpTooltip helpTextKey="aggregation-function" />
        </label>
        {showAggs ? (
          <Select
            placeholder="Use default if none selected"
            value={policy.aggFunctions}
            onChange={e => {
              handleChange('aggFunctions', e);
            }}
            autoComplete="off"
            mode="multiple"
            style={{width: '100%'}}
            tokenSeparators={[',']}
            notFoundContent={false}>
            {AGGREGATION_FUNCTIONS.map(opt => <Option key={opt}>{opt}</Option>)}
          </Select>
        ) : (
          <small style={{color: 'gray'}}>Only supported for timers</small>
        )}
      </div>
    </div>
  );
}

function PoliciesEditor(props) {
  const {value: policies, onChange, typeTag} = props;
  return (
    <div
      className="clearfix p1"
      style={{
        border: '1px solid whitesmoke',
        lineHeight: 'normal',
      }}>
      {_.map(policies, (p, i) => {
        return (
          <div
            key={i}
            className="clearfix relative mb1"
            style={{
              backgroundColor: 'whitesmoke',
              lineHeight: 'normal',
            }}>
            <PolicyEditor
              onDeleteClicked={() => {
                onChange(policies.filter((__, idx) => i !== idx));
              }}
              showDelete={_.size(policies) > 1}
              value={p}
              typeTag={typeTag}
              onChange={e => {
                const newPolicy = policies.slice();
                newPolicy[i] = e;
                onChange(newPolicy);
              }}
            />
          </div>
        );
      })}
      <div className="col col-12">
        <Button
          className="right"
          size="small"
          type="dashed"
          onClick={() => {
            onChange((policies || []).concat(_.first(POLICIES)));
          }}>
          Add Policy
        </Button>
      </div>
    </div>
  );
}

export default PoliciesEditor;
