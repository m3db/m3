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
import {message} from 'antd';
import NProgress from 'nprogress';
import _ from 'lodash';
import {connect} from 'react-refetch';

const BASE_URL = '/r2/v1';
function getErrorMessage(errorMessage) {
  return (
    <span className="h4">
      {errorMessage}
    </span>
  );
}
function customFetch(mapping) {
  const meta = _.get(mapping, 'meta', {});
  if (meta.loadingMessage || meta.successMessage) {
    message.destroy();
    message.loading(
      _.isString(meta.loadingMessage) ? meta.loadingMessage : 'Loading...',
      0,
    );
  }
  const options = {
    method: mapping.method,
    cache: 'force-cache',
    headers: mapping.headers,
    credentials: mapping.credentials,
    redirect: mapping.redirect,
    body: JSON.stringify(mapping.body),
  };
  const req = new Request(BASE_URL + mapping.url, options);

  NProgress.start();
  return fetch(req).then(response => {
    NProgress.done();
    if (!response.ok) {
      const clonedResponse = response.clone();
      clonedResponse.json().then(t => {
        message.destroy();
        message.error(getErrorMessage(`[${response.status}] ${t.message}`), 5);
      });
    } else if (meta.successMessage) {
      message.destroy();
      message.success(
        getErrorMessage(
          _.isString(meta.successMessage) ? meta.successMessage : 'Success!',
        ),
        1,
      );
    }
    return response;
  });
}

export const connectR2API = connect.defaults({
  fetch: customFetch,
  refetching: true,
  force: true,
  buildRequest: mapping => mapping,
});
