import Vue from 'vue'
import Router from 'vue-router'
import XHeader from '@/components/XHeader'
import XGraph from '@/components/XGraph'
import XList from '@/components/XList'
import XNodeAttributes from '@/components/XNodeAttributes'
import XTestNodeDimensions from '@/components/XTestNodeDimensions'

Vue.use(Router)

const router = new Router({
  routes: [
    {
      path: '/',
      component: XHeader,
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
        {
          path: 'test',
          name: 'XTestNodeDimensions.vue',
          component: XTestNodeDimensions,
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
// router.replace('/graph')
// router.replace({ path: '*', redirect: '/graph' })

export default router
