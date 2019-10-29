import Vue from 'vue';
import { library } from '@fortawesome/fontawesome-svg-core';
import { faEye } from '@fortawesome/free-regular-svg-icons/faEye';
import { faBullseye } from '@fortawesome/free-solid-svg-icons/faBullseye';
import { faSearch } from '@fortawesome/free-solid-svg-icons/faSearch';
import { faListUl } from '@fortawesome/free-solid-svg-icons/faListUl';
import { faPlusCircle } from '@fortawesome/free-solid-svg-icons/faPlusCircle';
import { faSitemap } from '@fortawesome/free-solid-svg-icons/faSitemap';
import { faFileCode } from '@fortawesome/free-solid-svg-icons/faFileCode';
import { faTimes } from '@fortawesome/free-solid-svg-icons/faTimes';
import { faFire } from '@fortawesome/free-solid-svg-icons/faFire';
import { faTrash } from '@fortawesome/free-solid-svg-icons/faTrash';
import { faExpandArrowsAlt } from '@fortawesome/free-solid-svg-icons/faExpandArrowsAlt';
import { faCompressArrowsAlt } from '@fortawesome/free-solid-svg-icons/faCompressArrowsAlt';
import { faFileAlt } from '@fortawesome/free-solid-svg-icons/faFileAlt';
import { faCaretUp } from '@fortawesome/free-solid-svg-icons/faCaretUp';
import { faCaretDown } from '@fortawesome/free-solid-svg-icons/faCaretDown';
import { faUndo } from '@fortawesome/free-solid-svg-icons/faUndo';
import { faExclamationCircle } from '@fortawesome/free-solid-svg-icons/faExclamationCircle';
import { faClock } from '@fortawesome/free-solid-svg-icons/faClock';
import { faQuestionCircle } from '@fortawesome/free-solid-svg-icons/faQuestionCircle';
import { faKeyboard } from '@fortawesome/free-solid-svg-icons/faKeyboard';
import { faBook } from '@fortawesome/free-solid-svg-icons/faBook';
import { faCogs } from '@fortawesome/free-solid-svg-icons/faCogs';
import { faClipboard } from '@fortawesome/free-solid-svg-icons/faClipboard';
import { faFileInvoice } from '@fortawesome/free-solid-svg-icons/faFileInvoice';
import { faArrowDown } from '@fortawesome/free-solid-svg-icons/faArrowDown';


import {
  FontAwesomeIcon, FontAwesomeLayers, FontAwesomeLayersText,
} from '@fortawesome/vue-fontawesome';

library.add(faBullseye, faEye, faSearch, faListUl, faPlusCircle,
  faSitemap, faFileCode, faTimes, faFire, faTrash,
  faExpandArrowsAlt, faCompressArrowsAlt, faFileAlt, faCaretUp, faCaretDown,
  faUndo, faExclamationCircle, faClock, faQuestionCircle, faKeyboard, faBook,
  faCogs, faClipboard, faFileInvoice, faArrowDown);

Vue.component('font-awesome-icon', FontAwesomeIcon);
Vue.component('font-awesome-layers', FontAwesomeLayers);
Vue.component('font-awesome-layers-text', FontAwesomeLayersText);
