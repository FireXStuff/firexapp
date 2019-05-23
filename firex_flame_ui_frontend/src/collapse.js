import _ from 'lodash';
import { getDescendantUuids } from './utils';

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
    let nodesToConsiderByUuid;

    // If an operation has a source_node, only find nodes that are descendants of that
    // source_node.
    if (_.has(displayConfig, ['source_node', 'value'])) {
      const uuidsToConsider = getDescendantUuids(displayConfig.source_node.value, nodesByUuid);
      uuidsToConsider.push(displayConfig.source_node.value);
      nodesToConsiderByUuid = _.pick(nodesByUuid, uuidsToConsider);
    } else {
      nodesToConsiderByUuid = nodesByUuid;
    }

    if (displayConfig.relative_to_nodes.type === 'task_name') {
      // TODO: consider adding support for long_name, though it isn't currently sent.
      const tasksWithName = _.filter(nodesToConsiderByUuid,
        n => n.name === displayConfig.relative_to_nodes.value);
      uuids = _.map(tasksWithName, 'uuid');
    } else if (displayConfig.relative_to_nodes.type === 'task_name_regex') {
      const regex = new RegExp(displayConfig.relative_to_nodes.value);
      const tasksWithNameMatchingRegex = _.filter(nodesToConsiderByUuid, n => regex.test(n.name));
      uuids = _.map(tasksWithNameMatchingRegex, 'uuid');
    } else if (displayConfig.relative_to_nodes.type === 'task_uuid') {
      uuids = [displayConfig.relative_to_nodes.value];
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
//  operation to the core collapse resolution algorithm, then sending simpler operations from here.
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

function getOpsAndDistanceForTarget(uuids, collapseOpsByUuid, target) {
  // Note we map before we filter so that we have the real distance, not the distance of only
  // nodes with operations.
  const uuidsToDistance = _.keyBy(_.map(uuids, (u, i) => ({ uuid: u, distance: i + 1 })),
    'uuid');
  const opsWithTargetByUuid = _.mapValues(collapseOpsByUuid,
    ops => _.filter(ops, o => _.includes(o.targets, target)));
  const opsAndDistancesByUuid = _.mapValues(opsWithTargetByUuid,
    (ops, uuid) => _.map(ops,
      o => _.merge({ distance: uuidsToDistance[uuid].distance }, o)));
  return _.flatten(_.values(opsAndDistancesByUuid));
}

function getAllOpsAffectingCollapseState(
  curNodeUuid, collapseOpsByUuid, curNodeAncestorUuids, curNodeDescendantUuids,
) {
  const expandCollapseInfluences = {
    self: _.filter(_.get(collapseOpsByUuid, curNodeUuid, []),
      op => _.includes(op.targets, 'self')),

    ancestor:
      getOpsAndDistanceForTarget(curNodeAncestorUuids,
        _.pick(collapseOpsByUuid, curNodeAncestorUuids), 'descendants'),

    descendant:
      getOpsAndDistanceForTarget(curNodeDescendantUuids,
        _.pick(collapseOpsByUuid, curNodeDescendantUuids), 'ancestors'),

    grandparent:
      getOpsAndDistanceForTarget(_.tail(curNodeAncestorUuids),
        _.pick(collapseOpsByUuid, _.tail(curNodeAncestorUuids)), 'grandchildren'),
  };
  const targetNameToPriority = {
    self: 1,
    ancestor: 2,
    descendant: 3,
    grandparent: 4,
  };
  // at otherwise equal priorities, expand beats collapse.
  const operationToPriority = {
    expand: 0,
    collapse: 1,
  };

  return _.flatMap(expandCollapseInfluences,
    (ops, targetName) => _.map(ops,
      op => _.merge({
        target: targetName,
        targetPriority: targetNameToPriority[targetName],
        opPriority: operationToPriority[op.operation],
      }, _.omit(op, ['targets']))));
}

function findMinPriorityOp(
  curNodeUuid, collapseOpsByUuid, curNodeAncestorUuids, curNodeDescendantUuids,
) {
  // TODO: optimize this function. It's more debug-friendly to find all affecting ops then
  // minimize, but this is slow in the presence of many collapse ops. Instead walk ops in
  // priority order and stop at first hit.
  const affectingOps = getAllOpsAffectingCollapseState(
    curNodeUuid,
    collapseOpsByUuid,
    curNodeAncestorUuids,
    curNodeDescendantUuids,
  );

  // Unfortunately need to sort since minBy doesn't work like sortBy w.r.t. array of properties.
  const sorted = _.sortBy(affectingOps, ['priority', 'opPriority', 'targetPriority', 'distance']);
  // If there are any 'clear' operations, ignore previous ops from that datasource.
  return _.head(sorted);
}

function resolveCollapseStatusByUuid(rootUuid, graphDataByUuid, collapseOpsByUuid) {
  const resultNodesByUuid = {};
  let toCheckUuids = [rootUuid];
  while (toCheckUuids.length > 0) {
    const curUuid = toCheckUuids.pop();

    // assumes walking root-down (parent collapsed state already calced).
    const minPriorityOp = findMinPriorityOp(
      curUuid,
      collapseOpsByUuid,
      graphDataByUuid[curUuid].ancestorUuids,
      graphDataByUuid[curUuid].descendantUuids,
    );
    resultNodesByUuid[curUuid] = {
      // If no rules affect this node (minPriorityOp undefined), default to false (not collapsed).
      collapsed: _.isUndefined(minPriorityOp) ? false : minPriorityOp.operation === 'collapse',
      minPriorityOp,
    };
    toCheckUuids = toCheckUuids.concat(graphDataByUuid[curUuid].childrenUuids);
  }
  // TODO: this is unfortunately necessary b/c collapsing the root shows nothing.
  // Consider supporting 'collapse down' to nearest uncollapsed descendant?
  resultNodesByUuid[rootUuid].collapsed = false;

  return resultNodesByUuid;
}

const stackOffset = 12;
const stackCount = 2; // Always have 2 stacked behind the front.

export {
  prioritizeCollapseOps,
  resolveDisplayConfigsToOpsByUuid,
  resolveToggleOperation,
  stackOffset,
  stackCount,
  resolveCollapseStatusByUuid,
};
