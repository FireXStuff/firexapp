import Vue from 'vue'
import Router from 'vue-router'
import XParent from '@/components/XParent'
import XGraph from '@/components/XGraph'
import XList from '@/components/XList'
import XNodeAttributes from '@/components/XNodeAttributes'
import _ from 'lodash'

Vue.use(Router)

const router = new Router({
  routes: [
    {
      path: '/',
      component: XParent,
      props: (route) => ({logDir: route.query.logDir, flameServer: route.query.flameServer}),
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

router.beforeEach((to, from, next) => {
  // By default, keep the current logDir. This is the central key by which data is fetched.
  if (from.query.logDir && !to.query.logDir) {
    // Need to clone since next expects a mutable route.
    let newTo = _.clone(to)
    newTo.query.logDir = from.query.logDir
    next(newTo)
  } else {
    next()
  }
})

export default router
