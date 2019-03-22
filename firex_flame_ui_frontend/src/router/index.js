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
          meta: { supportedActions: ['support-list-link', 'support-center', 'support-help-link', 'support-watch'] },
        },
      ],
    },
  ],
  // always scroll to top.
  scrollBehavior (to, from, savedPosition) {
    return {x: 0, y: 0}
  },
})

// TODO: doesn't affect clicking to open a new tab.
router.beforeEach((to, from, next) => {
  // By default, keep the current logDir & flameServer. These are the central key by which data is fetched.
  let keptQuery = {}
  if (from.query.logDir && !to.query.logDir) {
    keptQuery.logDir = from.query.logDir
  }
  if (from.query.flameServer && !to.query.flameServer) {
    keptQuery.flameServer = from.query.flameServer
  }

  if (!_.isEmpty(keptQuery)) {
    let newTo = _.clone(to)
    newTo.query = _.assign({}, newTo.query, keptQuery)
    next(newTo)
  } else {
    next()
  }
})

export default router
