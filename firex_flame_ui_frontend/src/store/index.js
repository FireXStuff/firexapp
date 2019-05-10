import Vue from 'vue';
import Vuex from 'vuex';
import tasks from './modules/tasks';
import graph from './modules/graph';
import firexRunMetadata from './modules/firexRunMetadata';

Vue.use(Vuex);

const debug = process.env.NODE_ENV !== 'production';

export default new Vuex.Store({
  modules: {
    tasks,
    graph,
    firexRunMetadata,
  },
  strict: debug,
});
