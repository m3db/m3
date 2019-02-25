import _ from 'lodash';
import {withProps, withHandlers, compose} from 'recompose';
import qs from 'query-string';

export const withFilter = ({
  propMapper,
  propName,
  propField,
  queryParam = 'q',
  filterPropName = 'filter',
  filterChangeHandlerName = 'setFilter',
}) => {
  return compose(
    withProps(props => {
      const prop = propMapper(props);
      const filter = qs.parse(props.location.search.replace('?', ''))[
        queryParam
      ];

      return {
        [filterPropName]: filter,
        [propName]: _.isEmpty(filter)
          ? prop
          : _.filter(prop, n => n[propField].indexOf(filter) !== -1),
      };
    }),
    withHandlers({
      [filterChangeHandlerName]: props => filter => {
        props.history.replace({
          pathname: window.location.pathname,
          search: filter ? `?${queryParam}=${filter}` : '',
        });
      },
    }),
  );
};
