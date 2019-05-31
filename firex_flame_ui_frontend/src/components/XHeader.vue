<template>
    <div class="header">
      <div style="display: flex; flex-direction: row;">
        <div>
          <router-link :to="{ name: 'XGraph',
            query: {logDir: $route.query.logDir, flameServer: $route.query.flameServer}}">
            <img style='height: 36px;' src="../assets/firex_logo.png" alt="firex logo">
          </router-link>
        </div>
        <div class="uid">{{title}}</div>
        <div v-if="chain" class="flame-link"><b>{{chain}}</b></div>

        <a  v-if="isCiscoDeployment"
            :href="legacyUrl" class="flame-link" style="font-size: 16px;">
          🔥 Back to Legacy 🔥
        </a>

        <div style="margin-left: auto; display: flex;">

          <slot name="prebuttons" class="header-icon-button"></slot>

          <template v-for="link in links">
            <popper :key="link.name"
                    trigger="hover" :options="{ placement: 'bottom' }"
                    :disabled="!Boolean(link.title)">
              <div class="popper header-popover">{{link.title}}</div>
              <div slot="reference">
                <x-header-button :link="link"></x-header-button>
              </div>
            </popper>
          </template>
        </div>
      </div>
    </div>
</template>

<script>
import _ from 'lodash';
import { mapState } from 'vuex';
import Popper from 'vue-popperjs';
import 'vue-popperjs/dist/vue-popper.css';

import XHeaderButton from './XHeaderButton.vue';

export default {
  name: 'XHeader',
  components: { XHeaderButton, Popper },
  props: {
    title: { default: '' },
    links: { default: () => [], type: Array },
    legacyPath: { default: '' },
  },
  computed: {
    ...mapState({
      chain: state => state.firexRunMetadata.chain,
      centralServer: state => state.firexRunMetadata.centralServer,
    }),
    legacyUrl() {
      // If there is no flame server query parameter, assume the app is being served from a flame
      // server and make the url relative to the server root.
      // TODO: Not great reading flame server directly from route.
      const start = _.get(this.$route, 'query.flameServer', '');
      return `${start}${this.legacyPath}?noUpgrade=true`;
    },
    isCiscoDeployment() {
      return this.centralServer === 'http://firex.cisco.com';
    },
  },
};
</script>

<style scoped>

.header {
  background-color: #EEE;
  border-bottom: 1px solid #000;
}

.uid {
  font-family: 'Source Sans Pro',sans-serif;
  margin: 0 8px;
  padding: 0;
  white-space: nowrap;
  font-size: 20px;
  line-height: 40px;
  font-weight: normal;
}

.header-icon-button {
  padding: 0 4px;
  border-left: 1px solid #000;
  justify-content: end;
  font-size: 20px;
  line-height: 40px;
  cursor: pointer;
  color: #000;
}

.header-icon-button:hover {
    color: #2980ff;
}

a {
  color: #000;
}

a:hover {
    color: #2980ff;
}

.kill-button {
  color: #900;
}

.kill-button:hover {
  color: #fff;
  background: #900;
}

.header-popover {
  padding: 3px;
  font-size: 13px;
  line-height: 1em;
  /*background: white;*/
  border-color: black;
  border-radius: 0;
  box-shadow: 2px 1px 1px rgb(58, 58, 58);
}

</style>
