import Vue from 'vue';
import VueRouter from 'vue-router';
import XTasksParent from '../components/XTasksParent.vue';
import XHeaderedGraph from '../components/XHeaderedGraph.vue';
import XList from '../components/XList.vue';
import XTimeChart from '../components/XTimeChart.vue';
import XNodeAttributes from '../components/nodes/XNodeAttributes.vue';
import _ from 'lodash';
import XHelp from '../components/XHelp.vue';
import XSettings from '../components/XSettings.vue';
import XShortcuts from '../components/XShortcuts.vue';
import XFindFirexId from '../components/XFindFirexId.vue';
import XError from '../components/XError.vue';
import XLiveFileViewer from '../components/XLiveFileViewer.vue';
import XErrorsTable from '../components/XErrorsTable.vue';
import XDirectoryListing from '../components/XDirectoryListing.vue';
import XRunInputs from '../components/XRunInputs.vue';
import { BASE_URL } from '/src/utils';

Vue.use(VueRouter);

const router = new VueRouter({
  mode: 'history',
  base: BASE_URL,
  routes: [
    {
      path: '/find',
      name: 'FindFirexId',
      component: XFindFirexId,
    },
    {
      path: '/find/:inputFireXId(FireX-.*-\\d+)',
      name: 'RedirectFoundFirexId',
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
      props: route => ({ inputFlameServerUrl: route.query.flameServer }),
    },
    {
      path: '/error',
      name: 'error',
      component: XError,
      props: route => ({ message: route.query.message }),
    },
    {
      path: '/:inputFireXId(FireX-.*-\\d+)?',
      component: XTasksParent,
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
          children: [
            {
              path: 'root/:rootUuid',
              props: true,
            },
          ],
        },
        {
          path: 'tasks/:uuid/:selectedSection?/:selectedSubsection?',
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
        {
          path: 'live-file',
          name: 'live-file',
          component: XLiveFileViewer,
          props: route => ({ filepath: route.query.file, host: route.query.host }),
        },
        {
          path: 'errors-table',
          name: 'XErrorsTable',
          component: XErrorsTable,
          children: [
            {
              path: 'root/:rootUuid',
            },
          ],
        },
        {
          path: 'inputs',
          name: 'XRunInputs',
          component: XRunInputs,
        },
        {
          path: 'logs/:logRelPath(.*)',
          name: 'XLogsDir',
          component: XDirectoryListing,
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
      path: '*',
      redirect: '/find',
    },
  ],
  // always scroll to top.
  scrollBehavior() {
    return { x: 0, y: 0 };
  },
});

router.beforeEach((to, from, next) => {
  /*
   * Support legacy, hash (#) based urls
   * by removing the #/ prefix.
  */
  if (!_.isEmpty(window.location.hash)) {
    const appPath = window.location.hash.slice(1);
    window.location.hash = '';
    routeToPath(appPath, next);
  } else {
    next();
  }
});

function routeToPath(appPath, next) {
  // fake url just for parsing path and query parts.
  const url = new URL(`http://fake.com${appPath}`);
  const query = {};
  (new URLSearchParams(url.search)).forEach(
    (value, key) => {
      query[key] = value;
  });
  next({
    path: url.pathname,
    query,
    replace: true, // don't leave history state
  });
}

export default router;
