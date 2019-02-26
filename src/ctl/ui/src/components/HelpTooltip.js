import React from 'react';
import {Popover, Icon} from 'antd';
import {getHelpText} from 'utils/helpText';

export default function HelpTooltip({helpTextKey, title, ...rest}) {
  return (
    <Popover
      title={title}
      content={<div style={{maxWidth: 200}}>{getHelpText(helpTextKey)}</div>}
      trigger="click"
      {...rest}>
      <Icon
        style={{cursor: 'pointer', color: 'rgba(0,0,0,0.3)'}}
        type="question-circle-o"
      />
    </Popover>
  );
}
