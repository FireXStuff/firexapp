import Vue from 'vue';
import Router from 'vue-router';
import XTasksParent from '@/components/XTasksParent.vue';
import XHeaderedGraph from '@/components/XHeaderedGraph.vue';
import XList from '@/components/XList.vue';
import XTimeChart from '@/components/XTimeChart.vue';
import XNodeAttributes from '@/components/nodes/XNodeAttributes.vue';
import _ from 'lodash';
import XHelp from '@/components/XHelp.vue';
import XSettings from '@/components/XSettings.vue';
import XShortcuts from '@/components/XShortcuts.vue';
import XFindFirexId from '@/components/XFindFirexId.vue';

Vue.use(Router);

const router = new Router({
  routes: [
    {
      path: '/:inputFireXId(FireX-.*)?',
      component: XTasksParent,
      props: route => ({
        inputLogDir: route.query.logDir,
        inputFlameServer: route.query.flameServer,
        inputFireXId: route.params.inputFireXId,
      }),
      beforeEnter: (to, from, next) => {
        if (_.isNil(to.params.inputFireXId) && _.isNil(to.query.flameServer)) {
          // re-direct to find page if no source of run data supplied.
          next('/find');
        } else {
          next();
        }
      },
      children: [
        {
          path: 'list',
          name: 'XList',
          component: XList,
          props: route => ({
            sort: _.get(route.query, 'sort', 'alphabetical'),
            sortDirection: _.get(route.query, 'sortDirection', 'ascending'),
            runstates: _.split(_.get(route.query,
              'runstates', 'Completed,In-Progress'), ','),
          }),
        },
        {
          path: 'time-chart',
          name: 'XTimeChart',
          component: XTimeChart,
          props: route => ({
            sort: _.get(route.query, 'sort', 'runtime'),
            sortDirection: _.get(route.query, 'sortDirection', 'desc'),
          }),
        },
        {
          path: 'tasks/:uuid',
          name: 'XNodeAttributes',
          component: XNodeAttributes,
          props: true,
        },
        {
          path: 'root/:rootUuid',
          name: 'custom-root',
          component: XHeaderedGraph,
          props: true,
        },
        // default path must be last.
        {
          path: '',
          name: 'XGraph',
          component: XHeaderedGraph,
          props: true,
        },
      ],
    },
    {
      path: '/find',
      name: 'FindFirexId',
      component: XFindFirexId,
    },
    {
      path: '/help',
      name: 'XHelp',
      component: XHelp,
      props: true,
    },
    {
      path: '/shortcuts',
      name: 'XShortcuts',
      component: XShortcuts,
      props: true,
    },
    {
      path: '/settings',
      component: XSettings,
      props: route => ({ inputFlameServer: route.query.flameServer }),
    },
  ],
  // always scroll to top.
  scrollBehavior() {
    return { x: 0, y: 0 };
  },
});

// TODO: doesn't affect clicking to open a new tab.
router.beforeEach((to, from, next) => {
  // By default, keep the current logDir & flameServer. These are the central key by which data
  // is fetched.
  const keptQuery = {};
  if (from.query.logDir && !to.query.logDir) {
    keptQuery.logDir = from.query.logDir;
  }
  if (from.query.flameServer && !to.query.flameServer) {
    keptQuery.flameServer = from.query.flameServer;
  }

  if (!_.isEmpty(keptQuery)) {
    const newTo = _.clone(to);
    newTo.query = _.assign({}, newTo.query, keptQuery);
    next(newTo);
  } else {
    next();
  }
});

export default router;
