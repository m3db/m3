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
import {Button, Input, Form, Icon, Select, Card} from 'antd';
import {toClass} from 'recompose';
import _ from 'lodash';
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

function RollupRuleEditor({form, rollupRule, onSubmit}) {
  const {getFieldDecorator} = form;
  return (
    <Form
      onSubmit={e => {
        e.preventDefault();
        form.validateFields((err, values) => {
          if (!err) {
            onSubmit({
              ...rollupRule,
              ...values,
            });
          }
        });
      }}>
      <div className="clearfix">
        <div className="col col-12 px1">
          <FormItem colon={false} label="Metric Name" {...formItemLayout}>
            {getFieldDecorator('name', {
              rules: [
                {
                  required: true,
                },
              ],
            })(<Input autoComplete={false} />)}
          </FormItem>
        </div>
        <div className="col col-12 px1">
          <FormItem colon={false} label="Metric Filter" {...formItemLayout}>
            {getFieldDecorator('filter', {
              rules: [
                {
                  required: true,
                },
              ],
            })(<Input />)}
          </FormItem>
        </div>
        <div className="col col-12 px1">
          <FormItem label="Targets" colon={false} {...formItemLayout}>
            {getFieldDecorator('targets', {
              initialValue: [],
              rules: [
                {
                  required: true,
                },
              ],
            })(<TargetsEditor />)}
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
  const {value: targets = [], onChange, onBlur = noop => noop} = props;
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
            extra={
              <a
                onClick={() => {
                  onChange(_.filter(targets, (__, index) => index !== i));
                }}>
                <Icon type="delete" />
              </a>
            }>
            <label>Name</label>
            <Input
              onBlur={onBlur}
              value={t.name}
              onChange={e => handleChange(i, 'name', e.target.value)}
            />
            <label>Policy</label>
            <Select
              onChange={e => handleChange(i, 'policies', e)}
              value={t.policies}
              autoComplete="off"
              mode="tags"
              style={{width: '100%'}}
              tokenSeparators={[',']}
              notFoundContent={false}>
              {['1m:10d|Max'].map(opt =>
                <Option key={opt}>
                  {opt}
                </Option>,
              )}
            </Select>
            <label>Tags</label>
            <Select
              onBlur={onBlur}
              onChange={e => handleChange(i, 'tags', e)}
              value={t.tags}
              autoComplete="off"
              mode="tags"
              style={{width: '100%'}}
              tokenSeparators={[',']}
              notFoundContent={false}
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

export default Form.create({
  mapPropsToFields({rollupRule = {name: '', filter: '', targets: []}}) {
    return {
      name: {
        value: rollupRule.name,
      },
      filter: {
        value: rollupRule.filter,
      },
      targets2: {
        value: rollupRule.targets,
      },
      targets: {
        value: rollupRule.targets,
      },
    };
  },
})(RollupRuleEditor);
