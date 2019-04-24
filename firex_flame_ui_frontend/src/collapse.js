import _ from 'lodash';

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
    if (displayConfig.relative_to_nodes.type === 'task_name') {
      // const soughtNameParts = displayConfig.relative_to_nodes.value.split('.');
      const tasksWithName = _.filter(nodesByUuid, (n) => {
        // TODO: long_name isn't in main graph. Should it be just for this purpose?
        // For now, just allow queries on end name.
        // if (_.has(n, 'long_name')) {
        //   const taskNameParts = n.long_name.split('.');
        //   return _.isEqual(soughtNameParts, _.takeRight(taskNameParts,
        //   soughtNameParts.length));
        // }
        return n.name === displayConfig.relative_to_nodes.value;
      });
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

export {
  prioritizeCollapseOps,
  resolveDisplayConfigsToOpsByUuid,
};
