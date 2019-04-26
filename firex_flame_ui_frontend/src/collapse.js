import _ from 'lodash';
import { getPrioritizedTaskStateBackgrounds, durationString } from './utils';

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
  const minFirstStarted = _.min(_.map(node.allRepresentedNodeUuids,
    u => nodesByUuid[u].local_received));
  // TODO: wrong for in-progress tasts.
  const maxEndTime = _.max(_.map(node.allRepresentedNodeUuids,
    u => nodesByUuid[u].local_received
        + _.get(nodesByUuid[u], 'actual_runtime', 0)));
  return _.merge(node, {
    // TODO: send all uniq backgrounds, prioritized.
    backgrounds: getPrioritizedTaskStateBackgrounds(
      _.map(node.allRepresentedNodeUuids, u => _.get(nodesByUuid, [u, 'state'])),
    ),
    width: 200,
    height: 75,
    hosts: _.uniq(_.map(node.allRepresentedNodeUuids, u => nodesByUuid[u].hostname)),
    time: durationString(maxEndTime - minFirstStarted),
  });
}

export {
  prioritizeCollapseOps,
  resolveDisplayConfigsToOpsByUuid,
  createUiCollapseNode,
};
