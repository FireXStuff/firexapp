// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import App from './App'
import router from './router'
import AsyncComputed from 'vue-async-computed'
import { library } from '@fortawesome/fontawesome-svg-core'
import { faEye } from '@fortawesome/free-regular-svg-icons'
import { faBullseye, faSearch, faListUl, faPlusCircle, faWindowMaximize,
  faWindowMinimize, faSitemap, faFileCode, faTimes } from '@fortawesome/free-solid-svg-icons'
import { FontAwesomeIcon } from '@fortawesome/vue-fontawesome'

library.add(faBullseye, faEye, faSearch, faListUl, faPlusCircle, faWindowMaximize, faWindowMinimize, faSitemap,
  faFileCode, faTimes)
Vue.use(AsyncComputed)
Vue.config.productionTip = false

Vue.component('font-awesome-icon', FontAwesomeIcon)

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  components: { App },
  template: '<App/>',
})
