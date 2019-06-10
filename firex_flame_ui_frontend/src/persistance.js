import _ from 'lodash';

import { containsAll } from './utils';

const USER_CONFIGS_KEY = 'firexUserConfigs';

function readPathsFromLocalStorage(localStorageKey, paths) {
  try {
    const runLocalData = JSON.parse(localStorage[localStorageKey]);
    if (paths === '*') {
      return runLocalData;
    }
    return _.pick(runLocalData, paths);
  } catch (e) {
    // Delete bad persisted state, provide default.
    localStorage.removeItem(localStorageKey);
  }
  return {};
}

function addLocalStorageData(key, newData) {
  const storedData = readPathsFromLocalStorage(key, '*');
  const toStoreData = _.assign(storedData, newData);
  localStorage[key] = JSON.stringify(toStoreData);
}

function readPathFromLocalStorage(firexUid, path, _default) {
  return _.get(readPathsFromLocalStorage(firexUid, [path]), path, _default);
}

function readValidatedPathFromLocalStorage(localStorageKey, path, validator, _default) {
  const value = _.get(readPathsFromLocalStorage(localStorageKey, [path]), path, _default);
  if (validator(value)) {
    return value;
  }
  return _default;
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

function loadDisplayConfigs() {
  // TODO: validate display config.
  const validator = () => true;
  return readValidatedPathFromLocalStorage(USER_CONFIGS_KEY, 'displayConfigs', validator, []);
}

export {
  readPathsFromLocalStorage,
  addLocalStorageData,
  readPathFromLocalStorage,
  getLocalStorageCollapseConfig,
  readValidatedPathFromLocalStorage,
  loadDisplayConfigs,
  USER_CONFIGS_KEY,
};
