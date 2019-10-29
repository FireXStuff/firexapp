<template>
    <div style="display: flex; flex-direction: column;">
      <div class="header header-row" style="height: 100%;">
        <div>
          <router-link :to="runRouteFromName('XGraph')">
            <img style='height: 36px;' src="../assets/firex_logo.png" alt="firex logo">
          </router-link>
        </div>
        <div class="uid">{{title}}</div>
        <!-- Unfortuntate that this comp needs to know search is in the slot.-->
        <!-- TODO: find a better fix for when no space for search bar (i.e. long chain value)-->
        <div v-if="chain && !isSearchOpen" class="flame-link" style="flex: 1;">
          <b style="width: 100%;">{{centralTitle}}</b>
        </div>

        <div style="margin-left: auto; display: flex; align-items: center; height: 100%;">

          <slot name="prebuttons"></slot>

          <template v-for="link in links">
            <x-header-button :link="link" :key="link.name"></x-header-button>
          </template>
        </div>
      </div>
      <div v-if="finalShowCompletionReportLink" class="header-row"
           style="background: #d1ebf3; justify-content: center;">
          <a class="btn btn-primary" :href="completionReportUrl" role="button"
             style="margin: 0.5em;">
            <font-awesome-icon icon="file-invoice"></font-awesome-icon>
            View Completion Report
          </a>
      </div>
    </div>
</template>

<script>
import _ from 'lodash';
import { mapState, mapGetters } from 'vuex';

import XHeaderButton from './XHeaderButton.vue';

export default {
  name: 'XHeader',
  components: { XHeaderButton },
  props: {
    title: { default: '' },
    links: { default: () => [], type: Array },
    mainTitle: { default: null, type: String },
    showCompletionReportLink: { default: false, type: Boolean },
  },
  asyncComputed: {
    completionReportFileExists() {
      // Avoid HTTP request if we don't expect the completion report to exist yet.
      if (!this.completionReportUrl) {
        return null;
      }
      return fetch(this.completionReportUrl, { method: 'HEAD' }).then(r => r.ok, () => false);
    },
  },
  computed: {
    ...mapState({
      chain: state => state.firexRunMetadata.chain,
      logsDir: state => state.firexRunMetadata.logs_dir,
      isSearchOpen: state => state.tasks.search.isOpen,
      uiConfig: state => state.header.uiConfig,
    }),
    ...mapGetters({
      runRouteFromName: 'header/runRouteFromName',
      // logsUrl: 'header/logsUrl',
      hasIncompleteTasks: 'tasks/hasIncompleteTasks',
    }),
    centralTitle() {
      return this.mainTitle ? this.mainTitle : this.chain;
    },
    finalShowCompletionReportLink() {
      return Boolean(this.showCompletionReportLink && this.completionReportFileExists);
    },
    canCheckCompletionReportExists() {
      return Boolean(!this.hasIncompleteTasks
        && _.get(this.uiConfig, 'rel_completion_report_path', false)
        && this.logsDir);
    },
    completionReportUrl() {
      if (this.canCheckCompletionReportExists) {
        const basePath = this.logsDir.endsWith('/') ? this.logsDir : `${this.logsDir}/`;
        return basePath + this.uiConfig.rel_completion_report_path;
      }
      return null;
    },
  },
};
</script>

<style>

.header {
  background-color: #EEE;
  border-top: 1px solid #000;
  border-bottom: 1px solid #000;
}

.header-row {
  display: flex;
  flex-direction: row;
  align-items: center;
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
