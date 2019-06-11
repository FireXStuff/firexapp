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

        <div class="flame-link">
          <a  v-if="isCiscoDeployment" :href="legacyUrl"  style="font-size: 12px;">
            Back to Legacy
          </a>
        </div>

        <div style="margin-left: auto; display: flex;">

          <slot name="prebuttons"></slot>

          <template v-for="link in links">
            <x-header-button :link="link" :key="link.name"></x-header-button>
          </template>
        </div>
      </div>
    </div>
</template>

<script>
import _ from 'lodash';
import { mapState } from 'vuex';

import XHeaderButton from './XHeaderButton.vue';

export default {
  name: 'XHeader',
  components: { XHeaderButton },
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

</style>
