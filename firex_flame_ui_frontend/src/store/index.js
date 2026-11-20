import { createPinia } from 'pinia';

export { useTasksStore } from './modules/tasks';
export { useGraphStore } from './modules/graph';
export { useFirexRunMetadataStore } from './modules/firexRunMetadata';
export { useHeaderStore } from './modules/header';
export { useRouteStore } from './modules/route';

/*
 * Stores are defined by defineStore and registered lazily on first use, so this
 * only needs to create the pinia instance the app (or a test) will install.
 */
export function createFlamePinia() {
  return createPinia();
}

export default createFlamePinia;
