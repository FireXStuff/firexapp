import _ from 'lodash';
import { rollupTaskStatesBackground } from './utils';

function prioritizeCollapseOps(opsByUuid, stateSourceName) {
  const priorityByStateSource = {
    flameData: 5,
    userConfig: 4,
    runState: 3,
  };

  const priority = _.get(priorityByStateSource, stateSourceName, 6);
  return _.mapValues(opsByUuid, ops => _.map(ops,
    // Note that if the op already has a priority, it's respected.
    o => _.merge({ priority, stateSource: stateSourceName }, o)));
}

function resolveDisplayConfigsToOpsByUuid(displayConfigs, nodesByUuid) {
  const resolvedByNameConfigs = _.flatMap(displayConfigs, (displayConfig) => {
    let uuids;
    // TODO: if an operation has a source_node, only find nodes that are descendants of that
    // source_node.
    if (displayConfig.relative_to_nodes.type === 'task_name') {
      // TODO: consider adding support for long_name, though it isn't currently sent.
      const tasksWithName = _.filter(nodesByUuid,
        n => n.name === displayConfig.relative_to_nodes.value);
      uuids = _.map(tasksWithName, 'uuid');
    } else if (displayConfig.relative_to_nodes.type === 'task_uuid') {
      uuids = [displayConfig.relative_to_nodes.type];
    } else {
      // This is an error -- unknown relative_to_nodes.type
      uuids = [];
    }
    return _.map(uuids, u => _.merge({ task_uuid: u },
      _.pick(displayConfig, ['operation', 'targets'])));
  });
  return _.mapValues(_.groupBy(resolvedByNameConfigs, 'task_uuid'),
    ops => _.map(ops, o => _.pick(o, ['operation', 'targets'])));
}

function createUiCollapseNode(node, nodesByUuid) {
  // Properies for collapse nodes.
  const collapsedCount = node.allRepresentedNodeUuids.length;
  let size;
  if (collapsedCount === 1) {
    size = '1';
  } else if (collapsedCount < 15) {
    size = 'small';
  } else if (collapsedCount < 50) {
    size = 'medium';
  } else {
    size = 'large';
  }
  const sizeToProps = {
    1: { radius: 25, fontSize: 10 },
    small: { radius: 50, fontSize: 13 },
    medium: { radius: 90, fontSize: 16 },
    large: { radius: 120, fontSize: 20 },
  };
  return _.merge(node, {
    background: rollupTaskStatesBackground(
      _.map(node.allRepresentedNodeUuids, u => _.get(nodesByUuid, [u, 'state'])),
    ),
    radius: sizeToProps[size].radius,
    width: sizeToProps[size].radius * 2,
    height: sizeToProps[size].radius * 2,
    fontSize: sizeToProps[size].fontSize,
  });
}

export {
  prioritizeCollapseOps,
  resolveDisplayConfigsToOpsByUuid,
  createUiCollapseNode,
};
