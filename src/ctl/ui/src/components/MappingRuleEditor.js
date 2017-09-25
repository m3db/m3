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
import {Button, Input, Form, Select} from 'antd';

const FormItem = Form.Item;
const Option = Select.Option;
const formItemLayout = {
  labelCol: {
    xs: {span: 24},
    sm: {span: 6},
  },
  wrapperCol: {
    xs: {span: 24},
    sm: {span: 14},
  },
};
function MappingRuleEditor({mappingRule, form, onSubmit}) {
  const {getFieldDecorator} = form;
  return (
    <Form
      layout="horizontal"
      onSubmit={e => {
        e.preventDefault();
        form.validateFields((err, values) => {
          if (!err) {
            onSubmit({
              ...mappingRule,
              ...values,
            });
          }
        });
      }}>
      <div className="clearfix">
        <div className="col col-12">
          <FormItem colon={false} label="Name" {...formItemLayout}>
            {getFieldDecorator('name', {
              rules: [
                {
                  required: true,
                },
              ],
            })(<Input autoComplete="off" />)}
          </FormItem>
        </div>
        <div className="col col-12">
          <FormItem
            colon={false}
            label="Metric Filter"
            {...formItemLayout}
            help="eg. tag1:value1 tag2:value2">
            {getFieldDecorator('filter', {
              rules: [
                {
                  required: true,
                },
              ],
            })(<Input autoComplete="off" />)}
          </FormItem>
        </div>
        <div className="col col-12">
          <FormItem
            colon={false}
            label="Policies"
            {...formItemLayout}
            help="eg:1m:10d = 1 min granularity for 10 days">
            {getFieldDecorator('policies', {
              rules: [
                {
                  required: true,
                },
              ],
            })(
              <Select
                autoComplete="off"
                mode="tags"
                style={{width: '100%'}}
                tokenSeparators={[',']}
                notFoundContent={false}>
                {['1m:10d'].map(opt =>
                  <Option key={opt}>
                    {opt}
                  </Option>,
                )}
              </Select>,
            )}
          </FormItem>
        </div>
        <div className="clearfix my1">
          <Button type="primary" htmlType="submit" className="right">
            Add
          </Button>
        </div>
      </div>
    </Form>
  );
}

export default Form.create({
  mapPropsToFields({mappingRule = {}}) {
    return {
      name: {
        value: mappingRule.name,
      },
      filter: {
        value: mappingRule.filter,
      },
      policies: {
        value: mappingRule.policies,
      },
    };
  },
})(MappingRuleEditor);
