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
import ReactDOM from 'react-dom';
import {
  BrowserRouter as Router,
  Route,
  Link,
  withRouter,
  matchPath,
  Redirect,
} from 'react-router-dom';
import {Layout, Menu, Icon, LocaleProvider} from 'antd';
import enUS from 'antd/lib/locale-provider/en_US';

import 'antd/dist/antd.css';
import 'basscss/css/basscss.css';
import 'nprogress/nprogress.css';
import './index.css';

import NamespacesPage from 'pages/Namespaces';
import NamespacePage from 'pages/Namespace';
const {Sider, Content} = Layout;

const MenuSideBar = withRouter(({location}) => {
  let selectedKey;
  if (
    matchPath(location.pathname, {
      path: '/namespaces',
    })
  ) {
    selectedKey = 'namespaces';
  }

  return (
    <Menu theme="dark" selectedKeys={[selectedKey]}>
      <Menu.Item key="namespaces">
        <Link to="/namespaces">
          <Icon type="user" />
          <span>Namespaces</span>
        </Link>
      </Menu.Item>
      <Menu.Item key="api-doc">
        <a href="/public-files/swagger">
          <span>API Docs</span>
        </a>
      </Menu.Item>
    </Menu>
  );
});

function AppContainer() {
  return (
    <LocaleProvider locale={enUS}>
      <Router>
        <Layout style={{height: '100%'}}>
          <Sider collapsible className="pt2">
            <MenuSideBar />
          </Sider>
          <Layout>
            <Content
              style={{
                margin: '0px 8px',
                padding: 24,
                height: '100%',
                overflow: 'scroll',
              }}>
              <Route exact path="/namespaces" component={NamespacesPage} />
              <Route exact path="/namespaces/:id" component={NamespacePage} />
              <Route
                exact
                path="/namespaces/:id/:tab"
                component={NamespacePage}
              />

              <Route
                exact
                path="/"
                render={() => <Redirect to="/namespaces" />}
              />
            </Content>
          </Layout>
        </Layout>
      </Router>
    </LocaleProvider>
  );
}

ReactDOM.render(<AppContainer />, document.getElementById('root'));
