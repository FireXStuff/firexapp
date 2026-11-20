import { defineStore } from 'pinia';

/*
 * Replaces vuex-router-sync: makes the current route readable from stores
 * (which, unlike components, have no $route). Kept in sync by the
 * router.afterEach hook installed in main.js.
 */
export const useRouteStore = defineStore('route', {
  state: () => ({
    name: null,
    path: '/',
    params: {},
    query: {},
  }),

  actions: {
    setRoute(route) {
      this.name = route.name;
      this.path = route.path;
      this.params = route.params;
      this.query = route.query;
    },
  },
});

export default useRouteStore;
