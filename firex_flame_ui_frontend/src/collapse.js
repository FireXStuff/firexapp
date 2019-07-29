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

function normalizeUiCollaseStateToOps(uiCollapseStateByUuid) {
  return _.mapValues(uiCollapseStateByUuid, uiState => (
    [{
      targets: [uiState.target],
      operation: uiState.operation,
      priority: uiState.priority,
      stateSource: 'ui',
    }]
  ));
}

function resolveDisplayConfigsToOpsByUuid(displayConfigs, nodesByUuid) {
  const resolvedByNameConfigs = _.flatMap(displayConfigs, (displayConfig) => {
    let uuids;
    let nodesToConsiderByUuid;

    // If an operation has a source_node, only find nodes that are descendants of that
    // source_node.
    if (_.has(displayConfig, ['source_node', 'value'])) {
      // TODO: This function should receive the pre-computed map from uuid to descendant list.
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
  curNodeUuid,
  collapseOpsByUuid,
  ancestorUuids,
  curNodeUnchainedAncestorUuids,
  curNodeDescendantUuids,
) {
  const expandCollapseInfluences = {
    self: _.filter(_.get(collapseOpsByUuid, curNodeUuid, []),
      op => _.includes(op.targets, 'self')),

    ancestor:
      getOpsAndDistanceForTarget(ancestorUuids,
        _.pick(collapseOpsByUuid, ancestorUuids), 'descendants'),

    unchainedAncestor:
      getOpsAndDistanceForTarget(curNodeUnchainedAncestorUuids,
        _.pick(collapseOpsByUuid, curNodeUnchainedAncestorUuids), 'unchained-descendants'),

    descendant:
      getOpsAndDistanceForTarget(curNodeDescendantUuids,
        _.pick(collapseOpsByUuid, curNodeDescendantUuids), 'ancestors'),

    // Unclear if it's worth support both chained and unchained verisons for grandchildren,
    // so disable for now.
    // grandparent:
    //   getOpsAndDistanceForTarget(_.tail(curNodeUnchainedAncestorUuids),
    //     _.pick(collapseOpsByUuid, _.tail(curNodeUnchainedAncestorUuids)), 'grandchildren'),
  };
  const targetNameToPriority = {
    self: 1,
    ancestor: 2,
    unchainedAncestor: 3,
    descendant: 4,
    // grandparent: 5,
  };
  // at otherwise equal priorities, expand beats collapse.
  const operationToPriority = {
    deny_child_collapse: 0, // Implies expand self.
    expand: 1,
    collapse: 2,
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
  curNodeUuid,
  collapseOpsByUuid,
  ancestorUuids,
  curNodeUnchainedAncestorUuids,
  curNodeDescendantUuids,
) {
  // TODO: optimize this function. It's more debug-friendly to find all affecting ops then
  // minimize, but this is slow in the presence of many collapse ops. Instead walk ops in
  // priority order and stop at first hit.
  const affectingOps = getAllOpsAffectingCollapseState(
    curNodeUuid,
    collapseOpsByUuid,
    ancestorUuids,
    curNodeUnchainedAncestorUuids,
    curNodeDescendantUuids,
  );

  // Unfortunately need to sort since minBy doesn't work like sortBy w.r.t. array of properties.
  const sorted = _.sortBy(affectingOps, ['priority', 'opPriority', 'targetPriority', 'distance']);
  // If there are any 'clear' operations, ignore previous ops from that datasource.
  return _.head(sorted);
}

function resolveNodeCollapseStatus(
  uuid, ancestorUuids, unchainedAncestorUuids, descendantUuids, collapseOpsByUuid,
) {
  // assumes walking root-down (parent collapsed state already calced).
  const minPriorityOp = findMinPriorityOp(
    uuid,
    collapseOpsByUuid,
    ancestorUuids,
    unchainedAncestorUuids,
    descendantUuids,
  );
  return {
    // If no rules affect this node (minPriorityOp undefined), default to false (not collapsed).
    collapsed: _.isUndefined(minPriorityOp) ? false : minPriorityOp.operation === 'collapse',
    minPriorityOp,
  };
}

function resolveCollapseStatusByUuid(rootUuid, graphDataByUuid, collapseOpsByUuid) {
  const resultNodesByUuid = {};
  let toCheckUuids = [rootUuid];
  while (toCheckUuids.length > 0) {
    const curUuid = toCheckUuids.pop();
    const parentMinOp = _.get(resultNodesByUuid,
      [graphDataByUuid[curUuid].parentId, 'minPriorityOp', 'operation'], null);

    let curNodeResult;
    if (parentMinOp === 'deny_child_collapse') {
      curNodeResult = { collapsed: false, minPriorityOp: { operation: 'parent_denies_collapse' } };
    } else {
      curNodeResult = resolveNodeCollapseStatus(
        curUuid,
        graphDataByUuid[curUuid].ancestorUuids,
        // If B is collapsed and chained before C, a rule that collapses B's descendants
        // SHOULD NOT collapse C. We therefore use the 'unchained' descendant structure.
        graphDataByUuid[curUuid].unchainedAncestorUuids,
        graphDataByUuid[curUuid].descendantUuids,
        collapseOpsByUuid,
      );
    }
    resultNodesByUuid[curUuid] = curNodeResult;
    toCheckUuids = toCheckUuids.concat(graphDataByUuid[curUuid].childrenUuids);
  }
  // TODO: this is unfortunately necessary b/c collapsing the root shows nothing.
  // Consider supporting 'collapse down' to nearest uncollapsed descendant?
  resultNodesByUuid[rootUuid].collapsed = false;

  return resultNodesByUuid;
}

function recursiveGetCollapseNodes(curUuid, childrenUuidsByUuid, parentId, isCollapsedByUuid) {
  const childResults = _.map(childrenUuidsByUuid[curUuid],
    childUuid => recursiveGetCollapseNodes(
      childUuid, childrenUuidsByUuid, curUuid, isCollapsedByUuid,
    ));
  const resultDescendantsByUuid = _.reduce(childResults, _.merge, {});

  const collapsedChildrenUuids = _.filter(childrenUuidsByUuid[curUuid],
    childUuid => isCollapsedByUuid[childUuid]);
  const collapsedDescendantUuids = _.flatMap(collapsedChildrenUuids,
    cUuid => [cUuid].concat(resultDescendantsByUuid[cUuid].collapsedUuids));

  // Every uncollapsed child of a collapsed child (i.e. uncollapsed grandchildren whose parents
  // are collapsed) need to have this node set as their parent.
  _.each(collapsedChildrenUuids, (ccUuid) => {
    const uncollapsedGrandchildrenParentCollapsed = _.filter(childrenUuidsByUuid[ccUuid],
      grandchildUuid => !isCollapsedByUuid[grandchildUuid]);
    _.each(uncollapsedGrandchildrenParentCollapsed,
      (grandchildUuid) => { resultDescendantsByUuid[grandchildUuid].parentId = curUuid; });
  });

  return _.assign({
    [[curUuid]]: {
      collapsedUuids: collapsedDescendantUuids,
      parentId,
    },
  }, resultDescendantsByUuid);
}

function getCollapsedGraphByNodeUuid(rootUuid, childrenUuidsByUuid, isCollapsedByUuid) {
  // TODO: collapse operations on the root node are ignored. Could collapse 'down',
  //  though it's unclear if that's preferable.
  return recursiveGetCollapseNodes(rootUuid,
    childrenUuidsByUuid, null, isCollapsedByUuid);
}

function createUiCollapseOp(operation, target) {
  const priority = -(new Date()).getTime();
  return { target, operation, priority };
}

const stackOffset = 12;
const stackCount = 2; // Always have 2 stacked behind the front.

export {
  prioritizeCollapseOps,
  normalizeUiCollaseStateToOps,
  resolveDisplayConfigsToOpsByUuid,
  stackOffset,
  stackCount,
  resolveCollapseStatusByUuid,
  getCollapsedGraphByNodeUuid,
  createUiCollapseOp,
};
