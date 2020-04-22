import _ from 'lodash';

import { getChildrenUuidsByUuid, getGraphDataByUuid } from '../../graph-utils';
import {
  getPrioritizedTaskStateBackground, concatArrayMergeCustomizer, createRunStateExpandOperations,
} from '../../utils';
import {
  prioritizeCollapseOps, resolveDisplayConfigsToOpsByUuid, stackOffset, stackCount,
  resolveCollapseStatusByUuid, getCollapsedGraphByNodeUuid, normalizeUiCollaseStateToOps,
} from '../../collapse';
import { loadDisplayConfigs } from '../../persistance';

const graphState = {
  collapseConfig: {
    uiCollapseStateByUuid: {},
    hideSuccessPaths: false,
    applyDefaultCollapseOps: true,
  },
  liveUpdate: true,
  showTaskDetails: false,
  isFirstLayout: true,
};

// getters
const graphGetters = {

  // TODO: update incrementally with each new parent/child relationship.
  graph(state, getters, rootState, rootGetters) {
    // TODO: does it make sense to keep data for entire tree even when a custom root is selected?
    const parentUuidByUuid = _.mapValues(rootState.tasks.allTasksByUuid, 'parent_id');
    const childrenUuidsByUuid = getChildrenUuidsByUuid(parentUuidByUuid);
    const graphDataByUuid = getGraphDataByUuid(
      rootGetters['tasks/rootUuid'],
      parentUuidByUuid,
      childrenUuidsByUuid,
      rootGetters['tasks/chainDepthByUuid'],
    );
    return {
      parentUuidByUuid,
      childrenUuidsByUuid,
      graphDataByUuid,
    };
  },

  parentUuidByUuid: (state, getters) => getters.graph.parentUuidByUuid,

  childrenUuidsByUuid: (state, getters) => getters.graph.childrenUuidsByUuid,

  graphDataByUuid: (state, getters) => getters.graph.graphDataByUuid,

  childrenAndAdditionalUuidsByUuid: (state, getters, rootState, rootGetters) => _.mergeWith({},
    getters.graph.childrenUuidsByUuid,
    rootGetters['tasks/additionalChildrenByUuid'],
    concatArrayMergeCustomizer),

  parentAndAdditionalUuidsByUuid: (state, getters, rootState, rootGetters) => {
    // Initialize with an array containing 'real' (non-additional) parent.
    const additionalParentsByUuid = _.mapValues(getters.graph.parentUuidByUuid,
      (uuid) => {
        if (uuid) {
          return [uuid];
        } else {
          // Do not include null parent (e.g. root's parent) in list of uuids.
          return [];
        }
      });

    _.each(rootGetters['tasks/additionalChildrenByUuid'], (additionaChildren, pUuid) => {
      _.each(additionaChildren, (childUuid) => {
        if (_.has(additionalParentsByUuid, childUuid)) {
          additionalParentsByUuid[childUuid].push(pUuid);
        }
      });
    });
    return _.mapValues(additionalParentsByUuid, _.uniq);
  },

  flameDataDisplayOperationsByUuid: (state, getters, rootState, rootGetters) => {
    const displayPath = ['flame_data', '_default_display', 'value'];
    // TODO: Each task can send updates that should override previous op entries for that task.
    //  Do that filtering here.
    const flameDataOps = _.flatMap(rootGetters['tasks/flameDataAndNameByUuid'],
      n => _.get(n, displayPath, []));
    const resolvedOpsByUuid = resolveDisplayConfigsToOpsByUuid(flameDataOps,
      rootGetters['tasks/flameDataAndNameByUuid']);

    // The tree that ops from flameData operate on are different thanthe UI hierarchy. Specifically,
    // flame collapse ops operate on a tree that ignores chains-to-descendant relationships.
    // We therefore transform 'descendants' to 'unchained-descendants' for all targets from
    // flame data ops.
    return _.mapValues(resolvedOpsByUuid, ops => _.map(ops, (op) => {
      const transformedTargets = _.map(op.targets,
        (t) => {
          if (t === 'descendants') {
            return 'unchained-descendants';
          }
          return t;
        });
      return _.assign({}, op, { targets: transformedTargets });
    }));
  },

  userDisplayConfigOperationsByUuid: (state, getters, rootState, rootGetters) => {
    // TODO: note this state is coming from local storage, should use library for syncing
    // localStorage state with app (vuex) state.
    const displayConfigs = loadDisplayConfigs();
    return resolveDisplayConfigsToOpsByUuid(displayConfigs,
      rootGetters['tasks/flameDataAndNameByUuid']);
  },

  runStateExpandOperationsByUuid(__, ___, ____, rootGetters) {
    return createRunStateExpandOperations(rootGetters['tasks/runStateByUuid']);
  },

  showOnlyRunStateCollapseOperationsByUuid(state, getters, rootState) {
    const rootCollapse = {
      [[rootState.firexRunMetadata.root_uuid]]:
        [{ targets: ['descendants'], operation: 'collapse' }],
    };
    return _.assign({}, rootCollapse, getters.runStateExpandOperationsByUuid);
  },

  mergedCollapseStateSources: (state, getters) => {
    const enabledCollapseStateSources = [];
    if (state.collapseConfig.applyDefaultCollapseOps) {
      enabledCollapseStateSources.push(
        prioritizeCollapseOps(getters.flameDataDisplayOperationsByUuid, 'flameData'),
      );
      enabledCollapseStateSources.push(
        prioritizeCollapseOps(getters.userDisplayConfigOperationsByUuid, 'userConfig'),
      );
      enabledCollapseStateSources.push(
        prioritizeCollapseOps(getters.runStateExpandOperationsByUuid, 'runState'),
      );
    }
    if (state.collapseConfig.hideSuccessPaths) {
      enabledCollapseStateSources.push(
        prioritizeCollapseOps(
          getters.showOnlyRunStateCollapseOperationsByUuid, 'runState',
        ),
      );
    }
    enabledCollapseStateSources.push(
      normalizeUiCollaseStateToOps(state.collapseConfig.uiCollapseStateByUuid),
    );
    return _.mergeWith({}, ...enabledCollapseStateSources, concatArrayMergeCustomizer);
  },

  resolvedCollapseStateByUuid(state, getters, rootState, rootGetters) {
    const rootUuid = rootGetters['tasks/rootUuid'];
    if (!_.has(getters.graphDataByUuid, rootUuid)) {
      return {};
    }
    return resolveCollapseStatusByUuid(
      rootUuid,
      getters.graphDataByUuid,
      getters.mergedCollapseStateSources,
    );
  },

  isCollapsedByUuid: (state, getters) => _.mapValues(
    getters.resolvedCollapseStateByUuid, 'collapsed',
  ),

  collapsedNodeUuids: (state, getters) => _.keys(_.pickBy(getters.isCollapsedByUuid)),

  uncollapsedNodeUuids: (state, getters) => _.keys(_.omitBy(getters.isCollapsedByUuid)),

  uncollapsedGraphByNodeUuid: (state, getters, rootState, rootGetters) => {
    const uncollapsedGraphByUuid = _.pick(getCollapsedGraphByNodeUuid(
      rootGetters['tasks/rootUuid'],
      getters.childrenUuidsByUuid,
      getters.isCollapsedByUuid,
    ),
    getters.uncollapsedNodeUuids);
    return _.mapValues(uncollapsedGraphByUuid,
      collapseData => ({
        background: getPrioritizedTaskStateBackground(
          _.map(collapseData.collapsedUuids,
            u => rootGetters['tasks/runStateByUuid'][u].state),
        ),
        collapsedUuids: collapseData.collapsedUuids,
        // We don't want the width to vary if a node has collapsed children or not,
        // so pad the same either way.
        // x2 because we pad both left and right to keep front box centering.
        widthPadding: stackCount * stackOffset * 2,
        // Only pad vertical to make room for real stacked behind boxes.
        heightPadding: _.isEmpty(collapseData.collapsedUuids) ? 0 : stackCount * stackOffset,
        parent_id: collapseData.parentId,
      }));
  },

};

// actions
const actions = {
  toggleShowTaskDetails(context) {
    context.commit('toggleShowTaskDetails');
  },
  toggleLiveUpdate(context) {
    context.commit('toggleLiveUpdate');
  },
  setUiCollapseStateByUuid(context, uiCollapseOpsByUuid) {
    context.commit('setUiCollapseStateByUuid', uiCollapseOpsByUuid);
  },
  expandAll(context) {
    context.dispatch('setCollapseConfigWithDefaults', {});
  },
  restoreCollapseDefault(context) {
    context.dispatch('setCollapseConfigWithDefaults', { applyDefaultCollapseOps: true });
  },
  collapseSuccessPaths(context) {
    context.dispatch('setCollapseConfigWithDefaults', { hideSuccessPaths: true });
  },
  setCollapseConfigWithDefaults(context, partialConfig) {
    context.commit('setCollapseConfig', {
      hideSuccessPaths: _.get(partialConfig, 'hideSuccessPaths', false),
      uiCollapseStateByUuid: _.get(partialConfig, 'uiCollapseStateByUuid', {}),
      applyDefaultCollapseOps: _.get(partialConfig, 'applyDefaultCollapseOps', false),
    });
  },
};

// mutations
const mutations = {
  setUiCollapseStateByUuid(state, uiCollapseStateByUuid) {
    state.collapseConfig = Object.assign({},
      state.collapseConfig, { uiCollapseStateByUuid });
  },

  setCollapseConfig(state, collapseConfig) {
    state.collapseConfig = collapseConfig;
  },

  toggleLiveUpdate(state) {
    state.liveUpdate = !state.liveUpdate;
  },

  toggleShowTaskDetails(state) {
    state.showTaskDetails = !state.showTaskDetails;
  },

  setIsFirstLayout(state, newIsFirstLayout) {
    state.isFirstLayout = newIsFirstLayout;
  },

};

export default {
  namespaced: true,
  state: graphState,
  getters: graphGetters,
  actions,
  mutations,
};
