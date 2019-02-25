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
import {Breadcrumb, Card, Tabs, Button, Modal} from 'antd';
import {Link} from 'react-router-dom';
import _ from 'lodash';
import {compose, withProps, withReducer} from 'recompose';
import {connectR2API, withPromiseStateChangeCallback} from 'hocs';
import MappingRuleEditor from 'components/MappingRuleEditor';
import MappingRulesTable from 'components/MappingRulesTable';
import RollupRuleEditor from 'components/RollupRuleEditor';
import RollupRulesTable from 'components/RollupRulesTable';

const TabPane = Tabs.TabPane;

function Namespace(props) {
  const {namespaceID, mappingRules, rollupRules, loading} = props;
  return (
    <div>
      <div className="mb2">
        <h2>
          Namespace: {namespaceID}
        </h2>
        <Breadcrumb>
          <Breadcrumb.Item>
            <Link to="/namespaces">All Namespaces</Link>
          </Breadcrumb.Item>
          <Breadcrumb.Item>
            {namespaceID}
          </Breadcrumb.Item>
        </Breadcrumb>
      </div>
      <Card className="mb2" style={{minHeight: 500}}>
        <Modal
          title={props.modal.title}
          width={'60%'}
          visible={props.modal.open}
          footer={false}
          onCancel={() => props.setModal({open: false})}>
          {props.modal.content}
        </Modal>

        <Tabs
          tabPosition={'left'}
          activeKey={props.tab}
          onChange={activeKey => {
            props.history.push({
              pathname: `/namespaces/${namespaceID}/${activeKey}`,
            });
          }}>
          <TabPane tab="Mapping Rules" key="mapping-rules">
            <div className="mb2 clearfix">
              <div className="left">
                <h3>Mapping Rules</h3>
              </div>
              <div className="right">
                <Button
                  icon="plus"
                  onClick={() =>
                    props.setModal({
                      open: true,
                      title: 'New Mapping Rule',
                      content: (
                        <MappingRuleEditor
                          onSubmit={values => {
                            props.saveMappingRule(values);
                          }}
                        />
                      ),
                    })}>
                  New Mapping Rule
                </Button>
              </div>
            </div>

            <MappingRulesTable
              loading={loading}
              mappingRules={mappingRules}
              namespaceID={namespaceID}
              {...props}
            />
          </TabPane>
          <TabPane tab="Rollup Rules" key="rollup-rules">
            <div className="mb2 clearfix">
              <div className="left">
                <h3>Rollup Rules</h3>
              </div>
              <div className="right">
                <Button
                  icon="plus"
                  onClick={() =>
                    props.setModal({
                      open: true,
                      title: 'New Rollup Rule',
                      content: (
                        <RollupRuleEditor
                          onSubmit={values => {
                            props.saveRollupRule(values);
                          }}
                        />
                      ),
                    })}>
                  New Rollup Rule
                </Button>
              </div>
            </div>
            <RollupRulesTable
              loading={loading}
              rollupRules={rollupRules}
              namespaceID={namespaceID}
              {...props}
            />
          </TabPane>
        </Tabs>
      </Card>
    </div>
  );
}

export default compose(
  withProps(props => {
    return {
      namespaceID: props.match.params.id,
      tab: props.match.params.tab || 'mapping-rules',
    };
  }),
  connectR2API(props => {
    const namespaceFetch = {
      url: `/namespaces/${props.namespaceID}`,
      refreshing: true,
    };
    return {
      namespaceFetch: {
        ...namespaceFetch,
        force: false,
      },
      saveMappingRule: mappingRule => {
        const {namespaceID} = props;
        const isNewRule = !_.has(mappingRule, 'id');
        const method = isNewRule ? 'POST' : 'PUT';
        const urlPath = isNewRule ? '' : `/${mappingRule.id}`;
        return {
          saveMappingRuleFetch: {
            meta: {
              successMessage: 'Mapping rule saved!',
            },
            url: `/namespaces/${namespaceID}/mapping-rules${urlPath}`,
            method,
            body: mappingRule,
            force: true,
            refreshing: true,
            andThen: () => ({namespaceFetch}),
          },
        };
      },
      deleteMappingRule: mappingRule => {
        const {namespaceID} = props;
        return {
          namespaceFetch: {
            meta: {
              successMessage: true,
            },
            url: `/namespaces/${namespaceID}/mapping-rules/${mappingRule.id}`,
            method: 'DELETE',
            force: true,
            refreshing: true,
            andThen: () => ({namespaceFetch}),
          },
        };
      },
      saveRollupRule: rollupRule => {
        const {namespaceID} = props;
        const isNewRule = !_.has(rollupRule, 'id');
        const method = isNewRule ? 'POST' : 'PUT';
        const urlPath = isNewRule ? '' : `/${rollupRule.id}`;
        return {
          saveRollupRuleFetch: {
            meta: {
              successMessage: 'Mapping rule saved!',
            },
            url: `/namespaces/${namespaceID}/rollup-rules${urlPath}`,
            method,
            body: rollupRule,
            force: true,
            refreshing: true,
            andThen: () => ({namespaceFetch}),
          },
        };
      },
      deleteRollupRule: rollupRule => {
        const {namespaceID} = props;
        return {
          namespaceFetch: {
            meta: {
              successMessage: true,
            },
            url: `/namespaces/${namespaceID}/rollup-rules/${rollupRule.id}`,
            method: 'DELETE',
            force: true,
            refreshing: true,
            andThen: () => ({namespaceFetch}),
          },
        };
      },
    };
  }),
  withProps(props => {
    const {namespaceFetch} = props;
    return {
      loading: namespaceFetch.pending || namespaceFetch.refreshing,
      mappingRules: _.get(namespaceFetch.value, 'mappingRules', []),
      rollupRules: _.get(namespaceFetch.value, 'rollupRules', []),
    };
  }),
  withReducer(
    'modal',
    'setModal',
    (state, payload) => {
      return {
        ...state,
        title: payload.title || state.title,
        open: payload.open,
        content: payload.content || state.content,
      };
    },
    props => ({open: false, content: null}),
  ),
  withPromiseStateChangeCallback(
    ['saveMappingRuleFetch', 'saveRollupRuleFetch'],
    props => {
      if (
        props.saveMappingRuleFetch.fulfilled ||
        props.saveRollupRuleFetch.fulfilled
      ) {
        props.setModal({open: false});
      }
    },
  ),
)(Namespace);
