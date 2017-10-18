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
import {Table, Tag} from 'antd';
import _ from 'lodash';
import MappingRuleEditor from 'components/MappingRuleEditor';
import TableActions from './TableActions';
import {compose} from 'recompose';
import {connectR2API} from 'hocs';
import {formatTimestampMilliseconds} from 'utils';

const {Column} = Table;

function MappingRuleHistoryBase(props) {
  const loading = _.get(props.mappingRuleHistory, 'pending');
  const mappingRules = _.get(props.mappingRuleHistory, 'value.mappingRules');
  return (
    <MappingRulesTable
      rowKey={(__, index) => index}
      mappingRules={mappingRules}
      loading={loading}
      showActions={false}
    />
  );
}
export const MappingRuleHistory = compose(
  connectR2API(props => {
    const {mappingRuleID, namespaceID} = props;
    return {
      mappingRuleHistory: {
        url: `/namespaces/${namespaceID}/mapping-rules/${mappingRuleID}/history`,
      },
    };
  }),
)(MappingRuleHistoryBase);

function MappingRulesTable(props) {
  const {
    namespaceID,
    loading,
    mappingRules,
    showActions = true,
    rowKey = 'id',
    saveMappingRule,
    deleteMappingRule,
  } = props;
  return (
    <Table
      scroll={{x: 1300}}
      loading={loading}
      dataSource={mappingRules}
      rowKey={rowKey}
      locale={{emptyText: 'No mapping rules'}}>
      <Column fixed="left" title="Rule Name" dataIndex="name" width={100} />
      <Column
        title="Metric Filter"
        dataIndex="filter"
        render={filter => <code>{filter}</code>}
      />
      <Column
        title="Policies"
        dataIndex="policies"
        render={policies => {
          return _.map(policies, policy => <Tag key={policy}>{policy}</Tag>);
        }}
      />
      <Column
        title="Last Updated By"
        dataIndex="lastUpdatedBy"
        render={user => user || 'N/A'}
      />
      <Column
        title="Last Updated At (Local)"
        dataIndex="lastUpdatedAtMillis"
        render={timestamp => formatTimestampMilliseconds(timestamp)}
      />
      <Column
        title="Effective Time (Local)"
        dataIndex="cutoverMillis"
        render={cutoverMillis => formatTimestampMilliseconds(cutoverMillis)}
      />
      {showActions && (
        <Column
          width={200}
          fixed="right"
          title="Action"
          key="action"
          render={(__, mappingRule) => (
            <TableActions
              onEditClicked={() =>
                props.setModal({
                  open: true,
                  title: 'Edit Mapping Rule',
                  content: (
                    <MappingRuleEditor
                      mappingRule={mappingRule}
                      onSubmit={values => {
                        saveMappingRule(values);
                      }}
                    />
                  ),
                })}
              onDeleteClicked={() => deleteMappingRule(mappingRule)}
              onHistoryClicked={() =>
                props.setModal({
                  open: true,
                  title: 'Mapping Rule History',
                  content: (
                    <MappingRuleHistory
                      namespaceID={namespaceID}
                      mappingRuleID={mappingRule.id}
                    />
                  ),
                })}
            />
          )}
        />
      )}
    </Table>
  );
}

export default MappingRulesTable;
