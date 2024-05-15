<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">

    <x-header :title="uid"
              main-title="Run Input Arguments"
              :links="headerLinks"/>

    <div class="node-attributes">
      <x-section heading="Service Arguments">
        <x-key-value-viewer
          :key-values="inputs"
          :ordered-keys="serviceInputKeys"
          :expandAll="true"/>
      </x-section>
      <x-section heading="FireX Arguments">
        <x-key-value-viewer
          :key-values="inputs"
          :ordered-keys="infraInputKeys"
          :expandAll="true"/>
      </x-section>
      <x-section v-if="submissionCmdStr" heading="Submission Command">
        <pre style="white-space: pre;">{{ submissionCmdStr }}</pre>
      </x-section>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapGetters, mapState } from 'vuex';
import XKeyValueViewer from './XKeyValueViewer.vue';
import XHeader from './XHeader.vue';
import XSection from './XSection.vue';

const INFRA_KEYS = [
  'disable_blaze',
  'firex_requester',
  'no_email',
  'plugins',
  'sync',
  'type',
  'project',
  'group',
  'type',
  'submitter',
  'submit_app',
  'starter_uuid',
  'invoker_id',
  'invoker_system',
  'invoker_system_info',
];

const HIDDEN_KEYS = [
  'submit_app',
  'submitter',
];

export default {
  name: 'XRunInputs',
  components: { XKeyValueViewer, XHeader, XSection },
  props: {},
  computed: {
    ...mapGetters({
      graphViewHeaderEntry: 'header/graphViewHeaderEntry',
      listViewHeaderEntry: 'header/listViewHeaderEntry',
      timeChartViewHeaderEntry: 'header/timeChartViewHeaderEntry',
      runLogsViewHeaderEntry: 'header/runLogsViewHeaderEntry',
    }),
    ...mapState({
      logsDir: state => state.firexRunMetadata.logs_dir,
      uid: state => state.firexRunMetadata.uid,
    }),
    runJsonPath() {
      return `${this.logsDir}/run.json`;
    },
    inputs() {
      if (!this.runJson) {
        return {};
      }
      return this.runJson.inputs;
    },
    allArgs() {
      return _.keys(this.inputs);
    },
    serviceInputKeys() {
      return _.difference(this.allArgs, INFRA_KEYS);
    },
    infraInputKeys() {
      const infraKeys = _.difference(
        _.intersection(this.allArgs, INFRA_KEYS),
        HIDDEN_KEYS,
      );
      return _.filter(
        infraKeys,
        k => !_.includes([null, ''], this.inputs[k]),
      );
    },
    submissionCmdStr() {
      return _.reduce(
        _.get(this.runJson, 'submission_cmd', []),
        (cmdAccumulated, arg) => {
          const isExe = cmdAccumulated === '';
          const isOption = arg.startsWith('-');
          const isValue = !isExe && !isOption;

          let sep;
          if (isExe) {
            sep = '';
          } else if (isOption) {
            sep = ' \\\n    ';
          } else {
            sep = ' ';
          }

          let finalArg;
          if (isValue) {
            const quoteChar = "'";
            const escapedArg = _.replace(arg, quoteChar, `\\${quoteChar}`);
            finalArg = `${quoteChar}${escapedArg}${quoteChar}`;
          } else {
            finalArg = arg;
          }

          return `${cmdAccumulated}${sep}${finalArg}`.trim();
        }, '',
      );
    },
    headerLinks() {
      return [
        this.graphViewHeaderEntry,
        this.listViewHeaderEntry,
        this.timeChartViewHeaderEntry,
        this.runLogsViewHeaderEntry,
      ];
    },
  },
  asyncComputed: {
    runJson() {
      return fetch(
        this.runJsonPath,
      ).then(r => r.json(), () => null);
    },
  },
};
</script>

<style scoped>

</style>
