import _ from 'lodash';
import { defineStore } from 'pinia';

import { eventHub, createLinkedHtml, createLinkifyRegex } from '../../utils';
import { useFirexRunMetadataStore } from './firexRunMetadata';
import { useGraphStore } from './graph';
import { useRouteStore } from './route';

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

export const useHeaderStore = defineStore('header', {
  state: () => ({
    uiConfig: null,
  }),

  getters: {

    accessMode: state => _.get(state.uiConfig, 'access_mode', ''),

    inputFlameServerUrl() {
      if (this.accessMode === 'socketio-origin') {
        return window.location.origin;
      }
      if (this.accessMode === 'socketio-param') {
        return useRouteStore().query.flameServer;
      }
      return null;
    },

    flameServerUrl() {
      if (this.isDataKeyFlameUrl) {
        return this.inputFlameServerUrl;
      }
      return useFirexRunMetadataStore().flame_url;
    },

    inputFireXId: () => useRouteStore().params.inputFireXId,

    isDataKeyFlameUrl() {
      return _.includes(['socketio-origin', 'socketio-param'], this.accessMode);
    },

    isDataKeyFireXId() {
      return !this.isDataKeyFlameUrl && !_.isNil(this.inputFireXId);
    },

    taskDataKey() {
      if (this.isDataKeyFlameUrl) {
        return this.inputFlameServerUrl;
      }
      if (this.isDataKeyFireXId) {
        return this.inputFireXId;
      }
      return null; // Not all routes need a data source key, e.g. Help view.
    },

    runRouteParamsAndQuery(state) {
      const flameUrlFromQuery = _.get(state.uiConfig, 'access_mode', '') === 'socketio-param';
      const query = this.isDataKeyFlameUrl && flameUrlFromQuery
        ? { flameServer: this.taskDataKey } : {};
      const params = this.isDataKeyFireXId ? { inputFireXId: this.taskDataKey } : {};
      return { params, query };
    },

    runRouteFromName() {
      return name => _.merge({ name }, this.runRouteParamsAndQuery);
    },

    getTaskRoute() {
      return (taskUuid, section) => {
        let path = `/tasks/${taskUuid}`;
        if (section) {
          path += `/${section}`;
        }

        return _.merge(
          prependFirexIdPath(path, this.isDataKeyFireXId, this.taskDataKey),
          this.runRouteParamsAndQuery,
        );
      };
    },

    getLiveFileRoute() {
      return (file, host) => _.merge(
        prependFirexIdPath('/live-file', this.isDataKeyFireXId, this.taskDataKey),
        { query: { file, host } },
        this.runRouteParamsAndQuery,
      );
    },

    getRootLogsRoute() {
      return _.merge(
        prependFirexIdPath('/logs/', this.isDataKeyFireXId, this.taskDataKey),
        this.runRouteParamsAndQuery,
      );
    },

    getCustomRootRoute() {
      return newRootUuid => _.merge(
        prependFirexIdPath(`root/${newRootUuid}`, this.isDataKeyFireXId, this.taskDataKey),
        this.runRouteParamsAndQuery,
      );
    },

    listViewHeaderEntry() {
      return {
        name: 'list',
        to: this.runRouteFromName('XList'),
        icon: 'list-ul',
        title: 'List View',
      };
    },

    graphViewHeaderEntry() {
      return {
        name: 'graph',
        to: this.runRouteFromName('XGraph'),
        icon: 'sitemap',
        title: 'Main Graph',
      };
    },

    runLogsViewHeaderEntry(state) {
      const useBuiltinLogsDir = _.get(state.uiConfig, ['logs_serving', 'serve_mode'],
        null) !== 'google-bucket';
      let href;
      let to;
      if (useBuiltinLogsDir) {
        href = this.logsUrl;
        to = null;
      } else {
        href = null;
        to = this.getRootLogsRoute;
      }
      return {
        name: 'logs',
        href,
        to,
        title: 'All Logs',
        icon: 'folder-open',
      };
    },

    timeChartViewHeaderEntry() {
      return {
        name: 'time-chart',
        to: this.runRouteFromName('XTimeChart'),
        icon: 'clock',
        title: 'Time Chart',
      };
    },

    helpViewHeaderEntry() {
      return {
        name: 'help',
        to: this.runRouteFromName('XHelp'),
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

    liveUpdateToggleHeaderEntry() {
      return toggleFunction => ({
        name: 'liveUpdate',
        on: toggleFunction,
        toggleState: useGraphStore().liveUpdate,
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

    showTaskDetailsHeaderEntry() {
      return showTaskDetailsFunction => ({
        name: 'showTaskDetails',
        on: showTaskDetailsFunction,
        toggleState: useGraphStore().showTaskDetails,
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
        title: 'Kill Entire Run',
      };
    },

    inputsViewHeaderEntry() {
      return {
        name: 'inputs',
        to: this.runRouteFromName('XRunInputs'),
        icon: 'sign-in-alt',
        title: 'Run Input Arguments',
      };
    },

    logsUrl(state) {
      const firexRunMetadataStore = useFirexRunMetadataStore();
      const logsPath = firexRunMetadataStore.logs_dir;

      const logsServeMode = _.get(state.uiConfig, ['logs_serving', 'serve_mode'], null);

      if (logsServeMode === 'central-webserver') {
        let logsServer;
        if (firexRunMetadataStore.logs_server) {
          logsServer = firexRunMetadataStore.logs_server;
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
          const templateArgs = { firex_id: firexRunMetadataStore.uid };
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

    createLinkedHtml() {
      return (text, linkClass) => createLinkedHtml(
        text, this.linkifyRegex, linkClass,
      );
    },

  },

  actions: {
    setFlameUiConfig(newUiConfig) {
      this.uiConfig = newUiConfig;
    },
  },
});

export default useHeaderStore;
