// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue';
import AsyncComputed from 'vue-async-computed';
import './icons';
import VueClipboard from 'vue-clipboard2';

import router from './router';
import App from './App.vue';
import store from './store';

const debug = process.env.NODE_ENV !== 'production';
Vue.config.devtools = debug;
Vue.config.performance = debug;

Vue.use(AsyncComputed);
Vue.use(VueClipboard);

Vue.config.productionTip = false;

store.watch(state => state.firexRunMetadata.uid, (uid) => { document.title = uid; });

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  store,
  render: h => h(App),
});

// This can be used by fallback code to see if the flame UI has been fetched from central sources.
window.flameUiLoadStarted = true;
