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
import XError from '@/components/XError.vue';
import XLiveFileViewer from '@/components/XLiveFileViewer.vue';
import XErrorsTable from '@/components/XErrorsTable.vue';
import XDirectoryListing from '@/components/XDirectoryListing.vue';
import { getCookie, deleteCookie } from '../utils';

Vue.use(Router);

const CISCO_SSO_FRAGMENT_COOKIE_NAME = 'anchorvalue';

function routeFromCiscoSsoCookieOrNext(next) {
  const anchorValue = getCookie(CISCO_SSO_FRAGMENT_COOKIE_NAME);
  const hasCiscoSsoCookie = anchorValue && anchorValue.startsWith('#/');
  if (hasCiscoSsoCookie) {
    const pathFromAnchorValue = anchorValue.slice(1); //  Remove '#" preceding the path.
    // Delete the cookie to avoid future /find accesses incorrectly redirecting.
    deleteCookie(CISCO_SSO_FRAGMENT_COOKIE_NAME, '/', '.cisco.com');
    next(pathFromAnchorValue);
  } else {
    next();
  }
}

const router = new Router({
  routes: [
    {
      path: '/find',
      name: 'FindFirexId',
      component: XFindFirexId,
      // This is effectively the default route, so handle the fact Cisco SSO doesn't restore URL
      // Fragments. It's instead made available via a cookie, so route & delete cookie here if
      // it's present.
      beforeEnter: (to, from, next) => {
        routeFromCiscoSsoCookieOrNext(next);
      },
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
  ],
  // always scroll to top.
  scrollBehavior() {
    return { x: 0, y: 0 };
  },
});

export default router;
