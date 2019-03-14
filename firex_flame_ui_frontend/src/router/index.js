import Vue from 'vue'
import Router from 'vue-router'
import XHeader from '@/components/XHeader'
import XGraph from '@/components/XGraph'
import XList from '@/components/XList'
import XNodeAttributes from '@/components/XNodeAttributes'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      name: 'XHeader',
      component: XHeader,
      children: [
        {
          path: 'graph',
          name: 'XGraph',
          component: XGraph,
          props: (route) => ({logDir: route.query.logDir})
        },
        {
          path: 'list',
          name: 'XList',
          component: XList,
          props: true
        },
        {
          path: 'tasks',
          name: 'XNodeAttributes',
          component: XNodeAttributes,
          props: true
        }
      ]
    }
  ]
})
