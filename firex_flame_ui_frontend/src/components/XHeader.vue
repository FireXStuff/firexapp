<template>
    <div style="display: flex; flex-direction: column;">
      <div class="header header-3-cols">

        <div style="text-align: left;" >
          <div style="display: flex; align-items: center;">
            <router-link :to="runRouteFromName('XGraph')">
              <img style='height: 36px;' src="../assets/firex_logo.png" alt="firex logo">
            </router-link>
            <div class="uid" >{{title}}</div>
          </div>
        </div>

        <div style="text-align: center; margin: 0 auto;">
          <!-- Unfortuntate that this comp needs to know search is in the slot.-->
          <!-- TODO: find a better fix for when no space for search bar (i.e. long chain value)-->
          <div v-if="chain && !isSearchOpen" class="flame-link">
            <b>{{centralTitle}}</b>
          </div>
        </div>

        <div class="right">
            <slot name="prebuttons"></slot>
            <x-header-button
              :link="link"
              v-for="link in links"
              :key="link.name"
            />
        </div>
      </div>

      <div v-if="formatRevokeTime"
        class="header-row revoked-reason"
      >
          Run revoked (cancelled) at {{ formatRevokeTime }}
          <template v-if="revokedReason">
            with reason: &nbsp; <i> {{ revokedReason }} </i>
          </template>
      </div>

      <div v-if="finalShowCompletionReportLink" class="header-row"
           style="background: #d1ebf3; justify-content: center;">
          <a class="btn btn-primary" :href="completionReportUrl" role="button"
             style="margin: 0.5em;">
            <font-awesome-icon icon="file-invoice"/>
            View Completion Report
          </a>
      </div>
    </div>
</template>

<script>
import _ from 'lodash';
import { DateTime } from 'luxon';
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
      revokedReason: state => state.firexRunMetadata.revoke_reason,
      revokeTimestamp: state => state.firexRunMetadata.revoke_timestamp,
      isSearchOpen: state => state.tasks.search.isOpen,
      uiConfig: state => state.header.uiConfig,
    }),
    ...mapGetters({
      runRouteFromName: 'header/runRouteFromName',
      // logsUrl: 'header/logsUrl',
      hasIncompleteTasks: 'tasks/hasIncompleteTasks',
    }),
    formatRevokeTime() {
      if (!this.revokeTimestamp) {
        return null;
      }
      return DateTime.fromSeconds(this.revokeTimestamp)
        .toLocaleString({...DateTime.DATETIME_SHORT_WITH_SECONDS, timeZoneName: 'short'});
    },
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
  flex-direction: row;
  display: flex;
  align-items: center;
}

.header-3-cols {
  flex-direction: row;
  display: grid;
  grid-template-columns: 1fr auto 1fr;
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

.revoked-reason {
  color: #FFF;
  background: rgb(255, 68, 0);
  justify-content: center;
}

.right {
    display: flex;
    flex-direction: row;
    justify-content: flex-end;
    align-items: center;
    height: 100%;
}


</style>
