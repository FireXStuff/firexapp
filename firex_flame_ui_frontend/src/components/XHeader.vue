<template>
    <div class="header">
      <div style="display: flex; flex-direction: row; align-items: center; height: 100%;">
        <div>
          <router-link :to="runRouteFromName('XGraph')">
            <img style='height: 36px;' src="../assets/firex_logo.png" alt="firex logo">
          </router-link>
        </div>
        <div class="uid">{{title}}</div>
        <!-- Unfortuntate that this comp needs to know search is in the slot.-->
        <!-- TODO: find a better fix for when no space for search bar (i.e. long chain value)-->
        <div v-if="chain && !isSearchOpen" class="flame-link" style="flex: 1;">
          <b style="width: 100%;">{{chain}}</b>
        </div>

        <div style="margin-left: auto; display: flex; align-items: center; height: 100%;">

          <slot name="prebuttons"></slot>

          <template v-for="link in links">
            <x-header-button :link="link" :key="link.name"></x-header-button>
          </template>
        </div>
      </div>
    </div>
</template>

<script>
import { mapState, mapGetters } from 'vuex';

import XHeaderButton from './XHeaderButton.vue';

export default {
  name: 'XHeader',
  components: { XHeaderButton },
  props: {
    title: { default: '' },
    links: { default: () => [], type: Array },
  },
  computed: {
    ...mapState({
      chain: state => state.firexRunMetadata.chain,
      isSearchOpen: state => state.tasks.search.isOpen,
    }),
    ...mapGetters({
      runRouteFromName: 'header/runRouteFromName',
    }),
  },
};
</script>

<style>

.header {
  background-color: #EEE;
  border-top: 1px solid #000;
  border-bottom: 1px solid #000;
}

.header-entry {
  border-left: 1px solid #000;
}

.uid {
  font-family: 'Source Sans Pro',sans-serif;
  margin: 0 8px;
  padding: 0;
  white-space: nowrap;
  font-size: 20px;
  font-weight: normal;
}

</style>
