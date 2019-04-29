// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue';
import AsyncComputed from 'vue-async-computed';
import { library } from '@fortawesome/fontawesome-svg-core';
import { faEye } from '@fortawesome/free-regular-svg-icons';
import {
  faBullseye, faSearch, faListUl, faPlusCircle,
  faSitemap, faFileCode, faTimes, faFire, faTrash, faExpandArrowsAlt,
  faCompressArrowsAlt,
} from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/vue-fontawesome';
import VueClipboard from 'vue-clipboard2';

import router from './router';
import App from './App.vue';

library.add(faBullseye, faEye, faSearch, faListUl, faPlusCircle,
  faSitemap, faFileCode, faTimes, faFire, faTrash,
  faExpandArrowsAlt, faCompressArrowsAlt);
Vue.use(AsyncComputed);
Vue.use(VueClipboard);
Vue.config.productionTip = false;

Vue.component('font-awesome-icon', FontAwesomeIcon);

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  render: h => h(App),
});
