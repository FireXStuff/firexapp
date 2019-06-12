import _ from 'lodash';

function getChildrenUuidsByUuid(parentUuidByUuid) {
  const childrenUuidsByUuid = {};
  _.each(parentUuidByUuid, (parentuuid, childUuid) => {
    if (!_.has(childrenUuidsByUuid, childUuid)) {
      childrenUuidsByUuid[childUuid] = [];
    }
    // Root's parent is null, only want output keys to be real nodes (UUIDs).
    if (!_.isNil(parentuuid)) {
      if (!_.has(childrenUuidsByUuid, parentuuid)) {
        childrenUuidsByUuid[parentuuid] = [];
      }
      childrenUuidsByUuid[parentuuid].push(childUuid);
    }
  });
  return childrenUuidsByUuid;
}

function recursiveGetAncestorsByUuid(uuid, childrenUuidsByUuid, ancestorPath) {
  const childrenUuids = childrenUuidsByUuid[uuid];
  const curEntry = { [[uuid]]: ancestorPath };
  if (_.isEmpty(childrenUuids)) {
    return curEntry;
  }
  const childrenAncestorPath = _.concat(uuid, ancestorPath);
  return _.reduce(
    _.map(childrenUuids, cUuid => recursiveGetAncestorsByUuid(cUuid,
      childrenUuidsByUuid, childrenAncestorPath)),
    _.assign,
    curEntry,
  );
}

function getAncestorsByUuid(rootUuid, childrenUuidsByUuid) {
  return recursiveGetAncestorsByUuid(rootUuid, childrenUuidsByUuid, []);
}

function getDescendantsByUuid(uuid, childrenUuidsByUuid) {
  const childrenUuids = childrenUuidsByUuid[uuid];
  if (_.isEmpty(childrenUuids)) {
    return { [[uuid]]: [] };
  }
  const descendantDescendantsByUuid = _.reduce(
    _.map(childrenUuids, cUuid => getDescendantsByUuid(cUuid, childrenUuidsByUuid)),
    _.assign,
    {},
  );
  descendantDescendantsByUuid[uuid] = _.keys(descendantDescendantsByUuid);
  return descendantDescendantsByUuid;
}

function findChainRootUuid(parentUuidByUuid, chainDepthByUuid, uuid) {
  let uuidToCheck = uuid;
  while (uuidToCheck) {
    const uuidChainDepth = chainDepthByUuid[uuidToCheck];
    if (uuidChainDepth === 0) {
      return uuidToCheck;
    }
    const parentUuid = parentUuidByUuid[uuidToCheck];
    if (uuidChainDepth === 1) {
      // If a task has chain depth of 1, that means the previous task in the chain (it's parent)
      // is the first task in the chain, and therefore it's grandparent is the root of the chain.
      return parentUuidByUuid[parentUuid];
    }
    uuidToCheck = parentUuid;
  }
  return null;
}

function getUnchainedParentUuidByUuid(parentUuidByUuid, chainDepthByUuid) {
  return _.mapValues(parentUuidByUuid,
    (parentUuid, uuid) => {
      if (chainDepthByUuid[uuid] > 0) {
        return findChainRootUuid(parentUuidByUuid, chainDepthByUuid, uuid);
      }
      // UUIDs with chain_depth 0 already have the 'correct' parent_id.
      return parentUuid;
    });
}

/**
 * Creates a list of ancestors per UUID where if a node A executes a chain B | C | D,
 * the nodes in the chain are all considered immediate children of A. This means B is not
 * an ancestor of C, like in the ordinary hierarchy.
 *
 * @param rootUuid The UUID from which to start traversal.
 * @param parentUuidByUuid A mapping from node UUIDs to that node's parent UUID.
 * @param chainDepthByUuid A mapping from node UUID to that node's chain depth.
 * @returns {{}} see above.
 */
function getUnchainedAncestorsByUuid(rootUuid, parentUuidByUuid, chainDepthByUuid) {
  const unchainedParentUuidByUuid = getUnchainedParentUuidByUuid(parentUuidByUuid,
    chainDepthByUuid);
  const unchainedChildrenUuidsByUuid = getChildrenUuidsByUuid(unchainedParentUuidByUuid);
  return getAncestorsByUuid(rootUuid, unchainedChildrenUuidsByUuid);
}

function getGraphDataByUuid(rootUuid, parentUuidByUuid, inputChildrenUuidsByUuid,
  chainDepthByUuid) {
  let childrenUuidsByUuid;
  if (_.isNil(childrenUuidsByUuid)) {
    childrenUuidsByUuid = getChildrenUuidsByUuid(parentUuidByUuid);
  } else {
    childrenUuidsByUuid = inputChildrenUuidsByUuid;
  }

  // TODO: combine ancestor and descendant aggregation in to single walk.
  const descendantUuidsByUuid = getDescendantsByUuid(rootUuid, childrenUuidsByUuid);
  const unnchainedAncestorsByUuid = getUnchainedAncestorsByUuid(rootUuid, parentUuidByUuid,
    chainDepthByUuid);

  return _.mapValues(parentUuidByUuid, (__, uuid) => ({
    parentId: parentUuidByUuid[uuid],
    childrenUuids: childrenUuidsByUuid[uuid],
    unchainedAncestorUuids: unnchainedAncestorsByUuid[uuid],
    descendantUuids: descendantUuidsByUuid[uuid],
    isLeaf: childrenUuidsByUuid[uuid].length === 0,
  }));
}

export {
  getChildrenUuidsByUuid,
  getAncestorsByUuid,
  getDescendantsByUuid,
  getGraphDataByUuid,
};
