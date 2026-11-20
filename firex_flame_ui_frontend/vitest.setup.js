import { config } from '@vue/test-utils';
import AsyncComputed from 'vue-async-computed';
import { FontAwesomeIcon } from '@fortawesome/vue-fontawesome';

config.global.plugins = [AsyncComputed];
config.global.components = { 'font-awesome-icon': FontAwesomeIcon };

// jsdom has no scrollTo, which the router's scrollBehavior calls on every navigation.
window.scrollTo = () => {};
