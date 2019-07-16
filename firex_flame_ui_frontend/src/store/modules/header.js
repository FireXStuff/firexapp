import _ from 'lodash';

import { eventHub } from '../../utils';

const headerState = {
  uiConfig: null,
};

function prependFirexIdPath(path, isDataKeyFireXId, taskDataKey) {
  let resultPath;
  if (isDataKeyFireXId) {
    const sep = path.startsWith('/') ? '' : '/';
    resultPath = `/${taskDataKey}${sep}${path}`;
  } else {
    resultPath = path;
  }
  return { path: resultPath };
}

// getters
const headerGetters = {

  inputFlameServer: (state, getters, rootState) => {
    if (_.get(state.uiConfig, 'access_mode', '') === 'socketio-origin') {
      return window.location.origin;
    }
    return rootState.route.query.flameServer;
  },

  inputFireXId: (state, getters, rootState) => rootState.route.params.inputFireXId,

  isDataKeyFlameUrl: (state, getters) => !_.isNil(getters.inputFlameServer),

  isDataKeyFireXId(state, getters) {
    return !getters.isDataKeyFlameUrl && !_.isNil(getters.inputFireXId);
  },

  taskDataKey: (state, getters) => {
    if (getters.isDataKeyFlameUrl) {
      return getters.inputFlameServer;
    }
    if (getters.isDataKeyFireXId) {
      return getters.inputFireXId;
    }
    return null; // Not all routes need a data source key, e.g. Help view.
  },

  runRouteParamsAndQuery: (state, getters) => {
    const flameUrlFromQuery = _.get(state.uiConfig, 'access_mode', '') === 'socketio-param';
    const query = getters.isDataKeyFlameUrl && flameUrlFromQuery
      ? { flameServer: getters.taskDataKey } : {};
    const params = getters.isDataKeyFireXId ? { inputFireXId: getters.taskDataKey } : {};
    return { params, query };
  },

  runRouteFromName: (state, getters) => name => _.merge({ name }, getters.runRouteParamsAndQuery),

  getTaskRoute: (state, getters) => taskUuid => _.merge(
    prependFirexIdPath(`/tasks/${taskUuid}`, getters.isDataKeyFireXId, getters.taskDataKey),
    getters.runRouteParamsAndQuery,
  ),

  getCustomRootRoute: (state, getters) => newRootUuid => _.merge(
    prependFirexIdPath(`root/${newRootUuid}`, getters.isDataKeyFireXId, getters.taskDataKey),
    getters.runRouteParamsAndQuery,
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

// mutations
const mutations = {
  setFlameUiConfig(state, newUiConfig) {
    state.uiConfig = newUiConfig;
  },
};

export default {
  namespaced: true,
  state: headerState,
  getters: headerGetters,
  actions: {},
  mutations,
};
