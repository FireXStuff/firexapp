import Vue from 'vue';
import Vuex from 'vuex';
import tasks from './modules/tasks';
import graph from './modules/graph';
import firexRunMetadata from './modules/firexRunMetadata';
import header from './modules/header';
import { isDebug } from '../utils';

Vue.use(Vuex);

function defaultStoreOptions() {
  return {
    modules: {
      tasks,
      graph,
      firexRunMetadata,
      header,
    },
    strict: isDebug,
    devtools: isDebug,
  };
}

const defaultStore = new Vuex.Store(defaultStoreOptions());
export { defaultStore as default, defaultStoreOptions };
