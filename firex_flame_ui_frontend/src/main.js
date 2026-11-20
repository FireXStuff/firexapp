import { createApp, watch } from 'vue';
import AsyncComputed from 'vue-async-computed';
import { registerIcons } from './icons';
import './assets/bootstrap-firex-custom.min.css';

import router from './router';
import App from './App.vue';
import { createFlamePinia } from './store';
import { useFirexRunMetadataStore } from './store/modules/firexRunMetadata';
import { useRouteStore } from './store/modules/route';

import { isDebug } from './utils';

const app = createApp(App);
const pinia = createFlamePinia();

app.config.performance = isDebug;

app.use(pinia);
app.use(router);
app.use(AsyncComputed);
registerIcons(app);

// make router data accessible from stores (replaces vuex-router-sync).
const routeStore = useRouteStore(pinia);
router.afterEach((to) => { routeStore.setRoute(to); });

const firexRunMetadataStore = useFirexRunMetadataStore(pinia);
watch(() => firexRunMetadataStore.uid, (uid) => { document.title = uid; });

app.mount('#app');

// This can be used by fallback code to see if the flame UI has been fetched from central sources.
window.flameUiLoadStarted = true;
