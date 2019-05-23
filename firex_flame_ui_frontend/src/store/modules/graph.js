import _ from 'lodash';

import { getChildrenUuidsByUuid, getGraphDataByUuid } from '../../graph-utils';
import {
  getCollapsedGraphByNodeUuid, getPrioritizedTaskStateBackground, concatArrayMergeCustomizer,
  loadDisplayConfigs, createRunStateExpandOperations,
} from '../../utils';
import {
  prioritizeCollapseOps, resolveDisplayConfigsToOpsByUuid, stackOffset, stackCount,
  resolveCollapseStatusByUuid,
} from '../../collapse';

const graphState = {
  collapseConfig: {
    uiCollapseOperationsByUuid: {},
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

  flameDataDisplayOperationsByUuid: (state, getters, rootState, rootGetters) => {
    const displayPath = ['flame_data', '_default_display', 'value'];
    // TODO: Each task can send updates that should override previous op entries for that task.
    //  Do that filtering here.
    const ops = _.flatMap(rootGetters['tasks/flameDataAndNameByUuid'],
      n => _.get(n, displayPath, []));
    return resolveDisplayConfigsToOpsByUuid(ops, rootGetters['tasks/flameDataAndNameByUuid']);
  },

  userDisplayConfigOperationsByUuid: (state, getters) => {
    const displayConfigs = loadDisplayConfigs();
    return resolveDisplayConfigsToOpsByUuid(displayConfigs, getters.flameDataAndNameByUuid);
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
    enabledCollapseStateSources.push(state.collapseConfig.uiCollapseOperationsByUuid);
    return _.mergeWith({}, ...enabledCollapseStateSources, concatArrayMergeCustomizer);
  },

  resolvedCollapseStateByUuid(state, getters, rootState) {
    const rootUuid = rootState.firexRunMetadata.root_uuid;
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
  setCollapseOpsByUuid(context, uiCollapseOpsByUuid) {
    context.commit('setCollapseOpsByUuid', uiCollapseOpsByUuid);
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
      uiCollapseOperationsByUuid: _.get(partialConfig, 'uiCollapseOperationsByUuid', {}),
      applyDefaultCollapseOps: _.get(partialConfig, 'applyDefaultCollapseOps', false),
    });
  },
};

// mutations
const mutations = {
  setCollapseOpsByUuid(state, uiCollapseOpsByUuid) {
    const newUiCollapseOperationsByUuid = Object.assign({},
      state.collapseConfig.uiCollapseOperationsByUuid, uiCollapseOpsByUuid);
    state.collapseConfig = Object.assign({},
      state.collapseConfig, { uiCollapseOperationsByUuid: newUiCollapseOperationsByUuid });
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
