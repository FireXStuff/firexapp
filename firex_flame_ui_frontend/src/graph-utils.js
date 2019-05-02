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

function recursivegetDescendantsByUuid(uuid, childrenUuidsByUuid) {
  const childrenUuids = childrenUuidsByUuid[uuid];
  if (_.isEmpty(childrenUuids)) {
    return { [[uuid]]: [] };
  }
  const descendantDescendantsByUuid = _.reduce(
    _.map(childrenUuids, cUuid => recursivegetDescendantsByUuid(cUuid, childrenUuidsByUuid)),
    _.assign,
    {},
  );
  descendantDescendantsByUuid[uuid] = _.keys(descendantDescendantsByUuid);
  return descendantDescendantsByUuid;
}

function getDescendantsByUuid(rootUuid, childrenUuidsByUuid) {
  return recursivegetDescendantsByUuid(rootUuid, childrenUuidsByUuid);
}

function getGraphDataByUuid(rootUuid, parentUuidByUuid, inputChildrenUuidsByUuid) {
  let childrenUuidsByUuid;
  if (_.isNil(childrenUuidsByUuid)) {
    childrenUuidsByUuid = getChildrenUuidsByUuid(parentUuidByUuid);
  } else {
    childrenUuidsByUuid = inputChildrenUuidsByUuid;
  }

  // TODO: could combine ancestor and descendant aggregation in to single walk.
  const ancestorUuidsByUuid = getAncestorsByUuid(rootUuid, childrenUuidsByUuid);
  const descendantUuidsByUuid = getDescendantsByUuid(rootUuid, childrenUuidsByUuid);

  return _.mapValues(parentUuidByUuid, (__, uuid) => ({
    parentId: parentUuidByUuid[uuid],
    childrenUuids: childrenUuidsByUuid[uuid],
    ancestorUuids: ancestorUuidsByUuid[uuid],
    descendantUuids: descendantUuidsByUuid[uuid],
  }));
}

export {
  getChildrenUuidsByUuid,
  getAncestorsByUuid,
  getDescendantsByUuid,
  getGraphDataByUuid,
};
