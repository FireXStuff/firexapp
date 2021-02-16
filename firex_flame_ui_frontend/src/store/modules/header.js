import _ from 'lodash';

import { eventHub, createLinkedHtml, createLinkifyRegex } from '../../utils';

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

  accessMode: state => _.get(state.uiConfig, 'access_mode', ''),

  inputFlameServerUrl: (state, getters, rootState) => {
    if (getters.accessMode === 'socketio-origin') {
      return window.location.origin;
    }
    if (getters.accessMode === 'socketio-param') {
      return rootState.route.query.flameServer;
    }
    return null;
  },

  flameServerUrl: (state, getters, rootState) => {
    if (getters.isDataKeyFlameUrl) {
      return getters.inputFlameServerUrl;
    }
    return rootState.firexRunMetadata.flame_url;
  },

  inputFireXId: (state, getters, rootState) => rootState.route.params.inputFireXId,

  isDataKeyFlameUrl: (state, getters) => _.includes(
    ['socketio-origin', 'socketio-param'], getters.accessMode,
  ),

  isDataKeyFireXId(state, getters) {
    return !getters.isDataKeyFlameUrl && !_.isNil(getters.inputFireXId);
  },

  taskDataKey: (state, getters) => {
    if (getters.isDataKeyFlameUrl) {
      return getters.inputFlameServerUrl;
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

  getTaskRoute: (state, getters) => (taskUuid, section) => {
    let path = `/tasks/${taskUuid}`;
    if (section) {
      path += `/${section}`;
    }

    return _.merge(
      prependFirexIdPath(path, getters.isDataKeyFireXId, getters.taskDataKey),
      getters.runRouteParamsAndQuery,
    );
  },

  getLiveFileRoute: (state, getters) => (file, host) => _.merge(
    prependFirexIdPath('/live-file', getters.isDataKeyFireXId, getters.taskDataKey),
    { query: { file, host } },
    getters.runRouteParamsAndQuery,
  ),

  getRootLogsRoute: (state, getters) => _.merge(
    prependFirexIdPath('/logs/', getters.isDataKeyFireXId, getters.taskDataKey),
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

  runLogsViewHeaderEntry(state, getters) {
    const useBuiltinLogsDir = _.get(state.uiConfig, ['logs_serving', 'serve_mode'],
      null) !== 'google-bucket';
    let href;
    let to;
    if (useBuiltinLogsDir) {
      href = getters.logsUrl;
      to = null;
    } else {
      href = null;
      to = getters.getRootLogsRoute;
    }
    return {
      name: 'logs',
      href,
      to,
      title: 'All Logs',
      icon: 'folder-open',
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

  documentationHeaderEntry(state) {
    return {
      name: 'documentation',
      href: _.get(state.uiConfig, 'central_documentation_url', 'http://firex.cisco.com'),
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
      icon: 'skull-crossbones',
      title: 'Kill Entire Run',
    };
  },

  logsUrl(state, getters, rootState) {
    const logsPath = rootState.firexRunMetadata.logs_dir;

    const logsServeMode = _.get(state.uiConfig, ['logs_serving', 'serve_mode'], null);

    if (logsServeMode === 'central-webserver') {
      let logsServer;
      if (rootState.firexRunMetadata.logs_server) {
        logsServer = rootState.firexRunMetadata.logs_server;
      } else {
        logsServer = _.get(state.uiConfig, 'central_server', null);
      }
      if (!_.isNil(logsServer)) {
        return logsServer + logsPath;
      }
    }

    if (logsServeMode === 'google-cloud-storage') {
      const serverFormat = _.get(state.uiConfig, ['logs_serving', 'url_format'], null);
      if (!_.isNil(serverFormat)) {
        const templateOptions = { evaluate: null, interpolate: null };
        const templateArgs = { firex_id: rootState.firexRunMetadata.uid };
        return _.template(serverFormat, templateOptions)(templateArgs);
      }
    }

    // Default to relative serving. This includes logsServeMode === 'local-webserver' or
    // misconfigurations (e.g. central-webserver log serving without a central server configured)
    return logsPath;
  },

  linkifyRegex: state => createLinkifyRegex(
    _.get(state.uiConfig, 'linkify_prefixes', []),
  ),

  createLinkedHtml: (state, getters) => (text, linkClass) => createLinkedHtml(
    text, getters.linkifyRegex, linkClass,
  ),

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
