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
import {Button, Input, Form, Icon, Select, Card, Tag} from 'antd';
import {toClass} from 'recompose';
import _ from 'lodash';
import {withFormik} from 'formik';
import * as util from 'utils';
import PoliciesEditor from './PolicyEditor';
import {filterPoliciesBasedOnTag} from 'utils';

// @TODO Move to config service
const REQUIRED_TAGS = ['dc', 'env', 'service', 'type'];

const FormItem = Form.Item;
const formItemLayout = {
  labelCol: {
    xs: {span: 24},
    sm: {span: 4},
  },
  wrapperCol: {
    xs: {span: 24},
    sm: {span: 18},
  },
};

function removeRequiredTags(tags, requiredTags) {
  return _.filter(tags, t => !_.includes(requiredTags, t));
}

function addRequiredTags(tags, requiredTags) {
  return _.concat(tags, requiredTags);
}

function RollupRuleEditor({values, handleChange, handleSubmit, setFieldValue}) {
  const typeTag = util.getTypeTag(values.filter);
  return (
    <Form onSubmit={handleSubmit}>
      <div className="clearfix">
        <div className="col col-12 px1">
          <FormItem
            required
            colon={false}
            label="Rule Name"
            {...formItemLayout}>
            <Input
              name="name"
              autoComplete="off"
              value={values.name}
              onChange={handleChange}
            />
          </FormItem>
        </div>
        <div className="col col-12 px1">
          <FormItem
            required
            colon={false}
            label="Metric Filter"
            help="eg. tag1:value1 tag2:value2"
            {...formItemLayout}>
            <Input
              name="filter"
              autoComplete="off"
              value={values.filter}
              onChange={e => {
                const newTypeTag = util.getTypeTag(e.target.value);
                const targets = _.map(values.targets, t => ({
                  ...t,
                  policies: filterPoliciesBasedOnTag(t.policies, newTypeTag),
                }));
                setFieldValue('targets', targets);
                handleChange(e);
              }}
            />
          </FormItem>
        </div>
        <div className="col col-12 px1">
          <FormItem required label="Targets" colon={false} {...formItemLayout}>
            <TargetsEditor
              typeTag={typeTag}
              value={values.targets}
              onChange={e => setFieldValue('targets', e)}
            />
          </FormItem>
        </div>
        <div className="clearfix my2">
          <Button type="primary" htmlType="submit" className="right">
            Add
          </Button>
        </div>
      </div>
    </Form>
  );
}

const TargetsEditorBase = props => {
  const {value: targets = [], onChange, typeTag} = props;
  const handleChange = (index, property, newValue) => {
    targets[index][property] = newValue;
    onChange(targets);
  };
  return (
    <div>
      {_.isEmpty(targets) && <div>No targets</div>}
      {_.map(targets, (t, i) => {
        return (
          <Card
            className="mb1"
            key={i}
            title={t.name}
            extra={
              <a
                onClick={() => {
                  onChange(_.filter(targets, (__, index) => index !== i));
                }}>
                <Icon type="delete" />
              </a>
            }>
            <label>New Rollup Metric Name</label>
            <Input
              value={t.name}
              onChange={e => handleChange(i, 'name', e.target.value)}
            />
            <label>Rollup Tags</label>
            <div>
              {_.map(REQUIRED_TAGS, requiredTag => <Tag>{requiredTag}</Tag>)}
              <Select
                className="inline-block"
                placeholder="Additional Tags"
                style={{minWidth: 200, width: 'inherit'}}
                onChange={e =>
                  handleChange(i, 'tags', addRequiredTags(e, REQUIRED_TAGS))}
                value={removeRequiredTags(t.tags, REQUIRED_TAGS)}
                autoComplete="off"
                mode="tags"
                tokenSeparators={[',']}
                notFoundContent={false}
              />
            </div>
            <label>Policies</label>
            <PoliciesEditor
              typeTag={typeTag}
              value={t.policies}
              onChange={e => handleChange(i, 'policies', e)}
            />
          </Card>
        );
      })}
      <Button
        type="dashed"
        onClick={() =>
          onChange(targets.concat({name: '', tags: [], policies: []}))}>
        Add Target
      </Button>
    </div>
  );
};
const TargetsEditor = toClass(TargetsEditorBase);

export default withFormik({
  enableReinitialize: true,
  mapPropsToValues: ({rollupRule}) => {
    return rollupRule || {};
  },
  handleSubmit: (values, {props}) => {
    props.onSubmit({
      ...values,
      policies: _.map(values.policies, p => _.union(p.tags, REQUIRED_TAGS)),
    });
  },
})(RollupRuleEditor);
