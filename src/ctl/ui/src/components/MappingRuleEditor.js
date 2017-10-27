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
import {Button, Input, Form} from 'antd';
import {withFormik} from 'formik';
import * as util from 'utils';
import yup from 'yup';
import PoliciesEditor from './PolicyEditor';
import {filterPoliciesBasedOnTag} from 'utils';
import {getHelpText} from 'utils/helpText';
const schema = yup.object().shape({
  name: yup.string('Name filter is required').required(),
  filter: yup.string().required('Metric filter is required'),
});

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
function MappingRuleEditor({
  mappingRule,
  values,
  handleChange,
  handleSubmit,
  setFieldValue,
  ...rest
}) {
  const typeTag = util.getTypeTag(values.filter);

  return (
    <Form layout="horizontal" onSubmit={handleSubmit}>
      <div className="clearfix">
        <div className="col col-12">
          <FormItem
            required="true"
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
        <div className="col col-12">
          <FormItem
            required="true"
            colon={false}
            label="Metric Filter"
            {...formItemLayout}
            help={getHelpText('metric-filter')}>
            <Input
              name="filter"
              autoComplete="off"
              value={values.filter}
              onChange={e => {
                const newTypeTag = util.getTypeTag(e.target.value);
                setFieldValue(
                  'policies',
                  filterPoliciesBasedOnTag(values.policies, newTypeTag),
                );
                handleChange(e);
              }}
            />
          </FormItem>
        </div>
        <div className="col col-12">
          <FormItem
            colon={false}
            required={true}
            label="Policies"
            {...formItemLayout}
            help={getHelpText('policy')}>
            <PoliciesEditor
              typeTag={typeTag}
              value={values.policies}
              onChange={e => setFieldValue('policies', e)}
            />
          </FormItem>
        </div>
        <Button type="primary" htmlType="submit" className="right">
          Add
        </Button>
      </div>
    </Form>
  );
}

export default withFormik({
  enableReinitialize: true,
  mapPropsToValues: ({mappingRule}) => {
    return mappingRule || {};
  },
  handleSubmit: (values, {props}) => {
    props.onSubmit(values);
  },
  validationSchema: schema,
})(MappingRuleEditor);
