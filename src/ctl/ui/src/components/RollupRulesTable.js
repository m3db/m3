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
import {Table, Popover, Tag} from 'antd';
import _ from 'lodash';
import RollupRuleEditor from 'components/RollupRuleEditor';
import TableActions from './TableActions';
import {compose} from 'recompose';
import {connectR2API} from 'hocs';
const {Column} = Table;

function TargetPreview({target}) {
  const content = (
    <div>
      <div className="pb1">
        <label className="bold">Policies</label>
        <div>
          {target.policies.join(' ')}
        </div>
      </div>
      <div className="pb1">
        <label className="bold pb1">Tags</label>
        <div>
          {target.tags.join(', ')}
        </div>
      </div>
    </div>
  );
  return (
    <Popover
      placement="topLeft"
      title={
        <b>
          {target.name}
        </b>
      }
      content={content}
      trigger="click">
      <Tag>
        {target.name}
      </Tag>
    </Popover>
  );
}

function RollupRuleHistoryBase(props) {
  const loading = _.get(props.rollupRuleHistory, 'pending');
  const rollupRules = _.get(props.rollupRuleHistory, 'value');

  return (
    <RollupRulesTable
      rollupRules={rollupRules}
      loading={loading}
      showActions={false}
    />
  );
}

const RollupRuleHistory = compose(
  connectR2API(props => {
    const {rollupRuleID, namespaceID} = props;
    return {
      rollupRuleHistory: {
        url: `/namespaces/${namespaceID}/rollup-rules/${rollupRuleID}/history`,
      },
    };
  }),
)(RollupRuleHistoryBase);

export const MappingRuleHistory = compose(
  connectR2API(props => {
    const {mappingRuleID, namespaceID} = props;
    return {
      rollupRuleHistory: {
        url: `/namespaces/${namespaceID}/mapping-rules/${mappingRuleID}/history`,
      },
    };
  }),
)(RollupRuleHistoryBase);

function RollupRulesTable(props) {
  const {
    namespaceID,
    loading,
    rollupRules,
    showActions = true,
    rowKey = 'id',
    saveRollupRule,
    deleteRollupRule,
  } = props;
  return (
    <Table
      loading={loading}
      dataSource={rollupRules}
      rowKey={rowKey}
      locale={{emptyText: 'No rollup rules'}}>
      <Column title="Name" dataIndex="name" />
      <Column
        title="Metric Filter"
        dataIndex="filter"
        render={filter =>
          <pre>
            {filter}
          </pre>}
      />
      <Column
        title="Targets"
        dataIndex="targets"
        render={targets => {
          return _.map(targets, (target, i) =>
            <TargetPreview key={i} target={target} />,
          );
        }}
      />
      {showActions &&
        <Column
          width={200}
          fixed="right"
          title="Action"
          key="action"
          render={(__, rollupRule) =>
            <TableActions
              onEditClicked={() =>
                props.setModal({
                  open: true,
                  title: 'Edit Rollup Rule',
                  content: (
                    <RollupRuleEditor
                      rollupRule={rollupRule}
                      onSubmit={values => {
                        saveRollupRule(values);
                      }}
                    />
                  ),
                })}
              onDeleteClicked={() => deleteRollupRule(rollupRule)}
              onHistoryClicked={() =>
                props.setModal({
                  open: true,
                  title: 'Rollup Rule History',
                  content: (
                    <RollupRuleHistory
                      namespaceID={namespaceID}
                      rollupRuleID={rollupRule.id}
                    />
                  ),
                })}
            />}
        />}
    </Table>
  );
}

export default RollupRulesTable;
