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

// Possible cases:
//    All Collapsed via 'collapse descendants':
//      Action: remove 'collapse descendants' op causing full collapse.
//
//    All Collapsed via default:
//      Action: 'expand descendants', if user just wants to expand collapsed, click that.
//
//    All expanded, no default ops:
//      Action: 'collapse descendants'
//
//    All expanded, default ops expanded:
//      Action: remove 'expand self' descendants with this task UUID
//        -> restores default collapse
//
//    Default collapsed, some expanded (implies default ops)
//      Action: 'collapse descendants'
//
// TODO: based on how complex this function is, it's probably worth exploring adding a 'clear'
//  operation to the core collapse resolution algorithm, then emitting simpler events from here.
function resolveToggleOperation(toggledTaskUuid, allDescendantsCollapsed, allChildrenExpanded,
  uiCollapseOperationsByUuid) {
  let resolvedOperation;
  if (allDescendantsCollapsed) {
    const collapsedByExistingOp = _.some(
      _.get(uiCollapseOperationsByUuid, toggledTaskUuid, []),
      op => op.operation === 'collapse' && _.isEqual(op.targets, ['descendants']),
    );
    if (collapsedByExistingOp) {
      resolvedOperation = {
        uuids: [toggledTaskUuid],
        operation: 'clear',
        target: 'descendants',
      };
    } else {
      // descendants collapsed by default, expand all.
      // TODO: is this a safe assumption?
      resolvedOperation = {
        uuids: [toggledTaskUuid],
        operation: 'expand',
        target: 'descendants',
      };
    }
  } else if (allChildrenExpanded) {
    const uuidsExpandedFromDefaultByParent = _.keys(_.pickBy(
      uiCollapseOperationsByUuid,
      ops => _.some(ops, op => op.sourceTaskUuid === toggledTaskUuid
        && _.isEqual(op.targets, ['self'])
        && op.operation === 'expand'),
    ));
    if (!_.isEmpty(uuidsExpandedFromDefaultByParent)) {
      resolvedOperation = {
        uuids: uuidsExpandedFromDefaultByParent,
        operation: 'clear',
        target: 'self',
      };
    } else {
      const expandedByExistingOp = _.some(
        _.get(uiCollapseOperationsByUuid, toggledTaskUuid, []),
        op => op.operation === 'expand' && _.isEqual(op.targets, ['descendants']),
      );
      if (expandedByExistingOp) {
        // All expanded without any default ops to restore, so just collapse everything.
        resolvedOperation = {
          uuids: [toggledTaskUuid],
          operation: 'clear',
          target: 'descendants',
        };
      } else {
        // All expanded without any default ops to restore, so just collapse everything.
        resolvedOperation = {
          uuids: [toggledTaskUuid],
          operation: 'collapse',
          target: 'descendants',
        };
      }
    }
  } else {
    // Neither all collapsed nor all expanded -- default collapsed, some expanded.
    resolvedOperation = {
      uuids: [toggledTaskUuid],
      operation: 'collapse',
      target: 'descendants',
    };
  }
  return resolvedOperation;
}

export {
  prioritizeCollapseOps,
  resolveDisplayConfigsToOpsByUuid,
  resolveToggleOperation,
};
