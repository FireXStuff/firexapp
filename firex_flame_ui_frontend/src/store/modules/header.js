import _ from 'lodash';

import { eventHub } from '../../utils';

function firexRunRoute(routeBase, dataSourceKey) {
  const query = dataSourceKey.is_flame_url ? { flameServer: dataSourceKey.key } : {};
  const params = dataSourceKey.is_firex_id ? { inputFireXId: dataSourceKey.key } : {};

  const newVals = { query, params };
  if (routeBase.path && dataSourceKey.is_firex_id) {
    newVals.path = `/${dataSourceKey.key}/${routeBase.path}`;
  }

  return _.merge({}, routeBase, newVals);
}

// getters
const headerGetters = {

  inputFlameServer: (state, getters, rootState) => rootState.route.query.flameServer,

  inputFireXId: (state, getters, rootState) => rootState.route.params.inputFireXId,

  dataSourceKey: (state, getters) => {
    if (!_.isNil(getters.inputFlameServer)) {
      return { is_flame_url: true, key: getters.inputFlameServer };
    }
    if (!_.isNil(getters.inputFireXId)) {
      return { is_firex_id: true, key: getters.inputFireXId };
    }
    // TODO: store error state and send errors somewhere.
    // throw Error("No data source key provided -- can't fetch run data.")
    return {}; // Not all routes need a data source key, e.g. Help view.
  },

  runRouteFromName: (state, getters) => name => firexRunRoute({ name }, getters.dataSourceKey),

  getTaskRoute: (state, getters) => taskUuid => firexRunRoute(
    { path: `tasks/${taskUuid}` }, getters.dataSourceKey,
  ),

  getCustomRootRoute: (state, getters) => newRootUuid => firexRunRoute(
    { path: `root/${newRootUuid}` }, getters.dataSourceKey,
  ),

  listViewHeaderEntry(state, getters) {
    return {
      name: 'list',
      to: getters.runRouteFromName('XList'),
      icon: 'list-ul',
      title: 'List View',
    };
  },

  graphViewHeaderEntry(state, getters) {
    return {
      name: 'graph',
      to: getters.runRouteFromName('XGraph'),
      icon: 'sitemap',
      title: 'Main Graph',
    };
  },

  runLogsViewHeaderEntry(state, getters, rootState, rootGetters) {
    return {
      name: 'logs',
      href: rootGetters['firexRunMetadata/logsUrl'],
      text: 'Logs',
      icon: 'file-alt',
    };
  },

  timeChartViewHeaderEntry(state, getters) {
    return {
      name: 'time-chart',
      to: getters.runRouteFromName('XTimeChart'),
      icon: 'clock',
      title: 'Time Chart',
    };
  },

  helpViewHeaderEntry(state, getters) {
    return {
      name: 'help',
      to: getters.runRouteFromName('XHelp'),
      text: 'Help',
      icon: 'question-circle',
    };
  },

  documentationHeaderEntry(state, getters, rootState) {
    return {
      name: 'documentation',
      href: rootState.firexRunMetadata.central_documentation_url,
      text: 'Documentation',
      icon: 'book',
    };
  },

  liveUpdateToggleHeaderEntry(state, getters, rootState) {
    return toggleFunction => ({
      name: 'liveUpdate',
      on: toggleFunction,
      toggleState: rootState.graph.liveUpdate,
      icon: ['far', 'eye'],
      title: 'Live Update',
    });
  },

  centerHeaderEntry() {
    return {
      name: 'center',
      on: () => eventHub.$emit('center'),
      icon: 'bullseye',
      title: 'Center',
    };
  },

  showTaskDetailsHeaderEntry(state, getters, rootState) {
    return showTaskDetailsFunction => ({
      name: 'showTaskDetails',
      // on: () => this.$store.dispatch('graph/toggleShowTaskDetails'),
      on: showTaskDetailsFunction,
      toggleState: rootState.graph.showTaskDetails,
      icon: 'plus-circle',
      title: 'Show Details',
    });
  },

  killHeaderEntry() {
    return {
      name: 'kill',
      on: () => eventHub.$emit('revoke-root'),
      _class: 'kill-button',
      icon: 'times',
      title: 'Kill',
    };
  },
};

export default {
  namespaced: true,
  state: {},
  getters: headerGetters,
  actions: {},
  mutations: {},
};
