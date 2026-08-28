import Vue from 'vue'
import VueRouter from 'vue-router';
import AsyncComputed from 'vue-async-computed';
import FontAwesomeIcon from '@fortawesome/vue-fontawesome';

Vue.use(VueRouter);
Vue.use(AsyncComputed);
Vue.component('font-awesome-icon', FontAwesomeIcon);