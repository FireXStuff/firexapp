import _ from 'lodash';

import { containsAll } from './utils';

function readPathsFromLocalStorage(firexUid, paths) {
  try {
    const runLocalData = JSON.parse(localStorage[firexUid]);
    if (paths === '*') {
      return runLocalData;
    }
    return _.pick(runLocalData, paths);
  } catch (e) {
    // Delete bad persisted state, provide default.
    localStorage.removeItem(firexUid);
  }
  return {};
}

function addLocalStorageData(firexUid, newData) {
  const storedData = readPathsFromLocalStorage(firexUid, '*');
  const toStoreData = _.assign(storedData, newData);
  localStorage[firexUid] = JSON.stringify(toStoreData);
}

function readPathFromLocalStorage(firexUid, path, def) {
  return _.get(readPathsFromLocalStorage(firexUid, [path]), path, def);
}

function getLocalStorageCollapseConfig(firexUid) {
  const storedCollapseConfig = readPathFromLocalStorage(firexUid, 'collapseConfig');
  const expectedKeys = ['hideSuccessPaths', 'uiCollapseOperationsByUuid',
    'applyDefaultCollapseOps'];
  // TODO: fill in remaining validation of collapseConfig.
  const containsAllRequired = containsAll(_.keys(storedCollapseConfig), expectedKeys);
  const collapseByUuidContainsAllRequired = _.every(
    _.values(_.get(storedCollapseConfig, 'uiCollapseOperationsByUuid', {})),
    ops => _.every(ops, op => containsAll(_.keys(op),
      ['operation', 'priority', 'targets', 'sourceTaskUuid'])),
  );
  if (containsAllRequired && collapseByUuidContainsAllRequired) {
    return storedCollapseConfig;
  }
  // Default collapse config.
  return {
    // This is just a container for states that have been touched by the user -- it doesn't
    // contain an entry for every node's UUID (there is a computed property for that).
    uiCollapseOperationsByUuid: {},
    // Hiding paths that don't include a failure or in progress.
    hideSuccessPaths: false,
    // Default to apply backend & user config display state.
    applyDefaultCollapseOps: true,
  };
}

export {
  readPathsFromLocalStorage,
  addLocalStorageData,
  readPathFromLocalStorage,
  getLocalStorageCollapseConfig,
};
