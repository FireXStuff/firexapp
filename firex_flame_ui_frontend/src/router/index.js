import Vue from 'vue'
import Router from 'vue-router'
import XParent from '@/components/XParent'
import XGraph from '@/components/XGraph'
import XList from '@/components/XList'
import XNodeAttributes from '@/components/XNodeAttributes'

Vue.use(Router)

const router = new Router({
  routes: [
    {
      path: '/',
      component: XParent,
      props: (route) => ({logDir: route.query.logDir}),
      children: [
        {
          path: 'list',
          name: 'XList',
          component: XList,
          props: true,
        },
        {
          path: 'tasks/:uuid',
          name: 'XNodeAttributes',
          component: XNodeAttributes,
          props: true,
        },
        // default path must be last.
        {
          path: '',
          name: 'XGraph',
          component: XGraph,
          props: true,
        },
      ],
    },
    // {
    //   path: '*', redirect: { name: 'XGraph' }
    // }
  ],
})

export default router
