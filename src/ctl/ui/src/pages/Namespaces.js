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
import {compose, withProps, withState} from 'recompose';
import {Button, Card, Input, Popconfirm, Table, Icon} from 'antd';
import _ from 'lodash';
import {Link} from 'react-router-dom';
import {connectR2API, withFilter} from 'hocs';

const {Column} = Table;

function NamespaceTable(props) {
  return (
    <Table
      rowKey="id"
      locale={{emptyText: 'No namespaces found, create one to get started!'}}
      loading={props.namespacesFetch.pending}
      dataSource={props.namespaces}>
      <Column
        sorter={(namespaceA, namespaceB) =>
          namespaceA.id.localeCompare(namespaceB.id)}
        title="Name"
        dataIndex="id"
        render={namespace => {
          return <Link to={`/namespaces/${namespace}`}>{namespace}</Link>;
        }}
      />
      <Column
        key="action"
        width={200}
        fixed="right"
        title="Action"
        render={(__, namespace) => (
          <Popconfirm
            placement="topRight"
            title={'Are you sure you want to delete?'}
            onConfirm={() => props.deleteNamespace(namespace)}
            okText="Yes"
            cancelText="No">
            <a>
              <Icon type="delete" />
            </a>
          </Popconfirm>
        )}
      />
    </Table>
  );
}

function CreateNamespaceFormBase(props) {
  const {namespace, onNamespaceChange, onSaveClick} = props;
  return (
    <div>
      <Input
        onPressEnter={onSaveClick}
        style={{width: 200}}
        className="inline-block mr1"
        value={namespace}
        onChange={e => onNamespaceChange(e.target.value)}
      />
      <Button
        disabled={_.isEmpty(namespace)}
        className="my1"
        onClick={onSaveClick}
        type="primary">
        Create Namespace
      </Button>
    </div>
  );
}

const CreateNamespaceForm = compose(
  withState('namespace', 'onNamespaceChange', ''),
  withProps(props => {
    return {
      onSaveClick: () => {
        props.onSaveNamespace(props.namespace);
        props.onNamespaceChange('');
      },
    };
  }),
)(CreateNamespaceFormBase);

function Namespaces(props) {
  return (
    <div>
      <div className="mb2">
        <h2>Namespaces</h2>
        <p style={{color: 'gray'}}>A list of all namespaces</p>
      </div>
      <Card>
        <Input
          className="mb1"
          value={props.nameFilter}
          onChange={e => props.setNameFilter(e.target.value)}
          placeholder="Namespace Filter"
        />
        <NamespaceTable {...props} />
        <CreateNamespaceForm onSaveNamespace={props.saveNamespace} />
      </Card>
    </div>
  );
}

export default compose(
  connectR2API(props => {
    const namespacesFetch = {url: '/namespaces', refreshing: true};
    return {
      namespacesFetch: {
        ...namespacesFetch,
        force: false,
      },
      saveNamespace: namespace => {
        return {
          saveNamespaceResponse: {
            meta: {
              successMessage: 'Namespace created!',
            },
            url: '/namespaces',
            method: 'POST',
            force: true,
            body: {
              id: namespace,
            },
            andThen: () => ({namespacesFetch}),
          },
        };
      },
      deleteNamespace: namespace => {
        return {
          deleteNamespaceResponse: {
            meta: {
              successMessage: 'Namespace deleted!',
            },
            url: `/namespaces/${namespace.id}`,
            method: 'DELETE',
            force: true,
            andThen: () => ({namespacesFetch}),
          },
        };
      },
    };
  }),
  withFilter({
    propMapper: props => _.get(props.namespacesFetch.value, 'namespaces', []),
    propName: 'namespaces',
    propField: 'id',
    queryParam: 'q',
    filterPropName: 'nameFilter',
    filterChangeHandlerName: 'setNameFilter',
  }),
)(Namespaces);
