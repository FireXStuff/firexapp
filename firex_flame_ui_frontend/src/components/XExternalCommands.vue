<template>
  <div>
    <x-section v-if="orderedIds.length > 1">
      <template v-slot:header><strong style="font-size: medium;">Summary</strong></template>
      <div>
        <router-link v-for="id in orderedIds" :key="'summary-' + id"
                     :to="{params: { selectedSubsection: id }}">
          <div style="display: flex; flex-direction: row;">
            <div>
              <x-external-command-status
                :display-external-command="displayExternalCommands[id]"
                :parent-task-complete="parentTaskComplete"
                :show-text="false"></x-external-command-status>
            </div>
            <div style="flex: 1;">{{summarizeCommand(displayExternalCommands[id].cmd)}}</div>
            <div style="align-self: flex-end;">{{displayExternalCommands[id].duration}}</div>
          </div>
        </router-link>
      </div>
    </x-section>

    <x-section v-for="id in orderedIds" :key="id" style="margin-bottom: 3em;"
      :id="'subsection' + id">
      <template v-slot:header>
        <div  style="display: inline; user-select: all; font-style: italic;">
          {{ displayExternalCommands[id].cwd }}</div>
        <strong style="font-size: medium;">$
          <span style="user-select: all;"
                v-html="createLinkedHtml(displayExternalCommands[id].cmd)"></span>

          <button v-if="isExpandableOutput(id)"
            class="btn btn-sm btn-primary pull-right" @click="expandedOutputIds.push(id)">
            <font-awesome-icon icon="expand-arrows-alt"></font-awesome-icon>
          </button>
          <router-link class="btn btn-link pull-right" :to="{params: { selectedSubsection: id }}">
            <font-awesome-icon icon="link"></font-awesome-icon>
          </router-link>
        </strong>
      </template>
      <div>
        <pre v-if="displayExternalCommands[id].result"
            style="background-color: #fafafa;"
            :style="{color: displayExternalCommands[id].result.returncode ? 'darkred' : '',
                     'max-height': isExpandedOutput(id) ? null : '10em'}"
            ref="outputs"
        >{{ displayExternalCommands[id].output }}</pre>
        <router-link
          v-else-if="!parentTaskComplete"
          :to="getLiveFileRoute(displayExternalCommands[id].file, displayExternalCommands[id].host)"
          class="btn btn-primary" style="margin: 1em 0;">
          <font-awesome-icon :icon="['far', 'eye']"></font-awesome-icon>
          View Live Output
        </router-link>
        <div class="row" style="margin-left: 2em">
            <div class="col-md-2">
              <strong>time: </strong> {{ displayExternalCommands[id].duration }}
            </div>

          <x-external-command-status
            :display-external-command="displayExternalCommands[id]"
            :parent-task-complete="parentTaskComplete"
          ></x-external-command-status>

          <div class="col-md-4">
            <strong>started: </strong> {{ displayExternalCommands[id].startTime }}
          </div>
          <div v-if="displayExternalCommands[id].taskLogLink" class="col-md-4">
            <a :href="displayExternalCommands[id].taskLogLink">
              <font-awesome-icon icon="file-alt"></font-awesome-icon>
              View in Task Log
            </a>
          </div>
        </div>
      </div>
    </x-section>
  </div>
</template>

<script>
import { DateTime } from 'luxon';
import _ from 'lodash';
import { mapGetters } from 'vuex';
import { durationString } from '../utils';
import XSection from './XSection.vue';
import XExternalCommandStatus from './XExternalCommandStatus.vue';

export default {
  name: 'XExternalCommands',
  components: { XExternalCommandStatus, XSection },
  props: {
    externalCommands: { required: true, type: Object },
    taskLogsUrl: { required: false, default: null },
    parentTaskEndTime: { type: Number },
  },
  data() {
    return {
      expandedOutputIds: [],
    };
  },
  mounted() {
    // Scroll output elements to bottom.
    _.each(_.get(this.$refs, 'outputs', []), (o) => { o.scrollTop = o.scrollHeight; });
  },
  computed: {
    ...mapGetters({
      getLiveFileRoute: 'header/getLiveFileRoute',
      createLinkedHtml: 'header/createLinkedHtml',
    }),
    displayExternalCommands() {
      return _.mapValues(this.externalCommands, (c, id) => ({
        cmd: this.formatCommand(c.cmd),
        result: c.result,
        output: this.formatOutput(c),
        duration: this.commandDuration(c),
        startTimeEpoch: c.start_time,
        startTime: this.formatTime(c.start_time),
        taskLogLink: this.taskLogsUrl ? `${this.taskLogsUrl}#${id}` : null,
        host: c.host,
        file: c.output_file,
        cwd: c.cwd,
      }));
    },
    orderedIds() {
      return _.sortBy(_.keys(this.externalCommands), k => this.externalCommands[k].start_time);
    },
    parentTaskComplete() {
      return !_.isNull(this.parentTaskEndTime);
    },
  },
  methods: {
    escapeCmdPart(cmdPart) {
      if (this.hasWhiteSpace(cmdPart)) {
        const escapedCmdPart = cmdPart.replace(/"/g, '\\"').replace(/'/g, "\\'");
        return `"${escapedCmdPart}"`;
      }
      return cmdPart;
    },
    hasWhiteSpace(s) {
      return /\s/g.test(s) || /'/g.test(s) || /"/g.test(s);
    },
    formatCommand(cmd) {
      if (_.isArray(cmd)) {
        return _.join(_.map(cmd, this.escapeCmdPart), ' ');
      }
      return cmd;
    },
    formatTime(unixTime) {
      return DateTime.fromSeconds(unixTime).toLocaleString(DateTime.DATETIME_FULL);
    },
    timeAgo(time) {
      return durationString(_.max([(Date.now() / 1000) - time, 0]));
    },
    commandDuration(cmd) {
      if (_.has(cmd, 'end_time')) {
        return durationString(cmd.end_time - cmd.start_time);
      }
      if (this.parentTaskComplete) {
        return durationString(this.parentTaskEndTime - cmd.start_time);
      }
      return this.timeAgo(cmd.start_time);
    },
    formatOutput(cmd) {
      if (!_.has(cmd, 'result.output')) {
        return null;
      }
      if (_.isNil(cmd.result.output)) {
        return '<output not captured>';
      }
      if (cmd.result.output === '') {
        return '<no output>';
      }
      if (cmd.result.output_truncated) {
        return `<truncated>\n...${cmd.result.output}`;
      }
      return cmd.result.output;
    },
    isExpandableOutput(cmdId) {
      const newlinesCount = _.countBy(this.displayExternalCommands[cmdId].output)['\n'] || 0;
      return newlinesCount > 6 && !this.isExpandedOutput(cmdId);
    },
    isExpandedOutput(cmdId) {
      return _.includes(this.expandedOutputIds, cmdId);
    },
    summarizeCommand(fullCommand) {
      const maxLength = 100;
      if (fullCommand.length < maxLength) {
        const parts = fullCommand.match(/\S+/g) || [];
        const first = _.last(_.split(parts[0], '/'));
        const rest = _.tail(parts).join(' ');
        return `${first} ${rest}`;
      }

      const lastCmd = _.last(_.split(fullCommand, ';'));

      const condensedParts = _.map(lastCmd.match(/\S+/g) || [], (cmdPart) => {
        // Only take the last part of absolute paths.
        if (_.startsWith(cmdPart, '/')) {
          return _.last(_.split(cmdPart, '/'));
        }
        // --work-dir=/some/long/absolute/path/lastbit -> --work-dir=lastbit
        if (_.includes(cmdPart, '=/')) {
          const argEqsParts = _.split(cmdPart, '=/', 2);
          const lastEqPathStart = _.last(argEqsParts);
          const truncPath = _.last(_.split(lastEqPathStart, '/'));
          return `${argEqsParts[0]}=[..]${truncPath}`;
        }

        return cmdPart;
      });

      const shortCommand = _.join(condensedParts, ' ');
      if (shortCommand.length > maxLength) {
        const start = _.dropRight(shortCommand, shortCommand.length - maxLength / 2).join('');
        const end = _.drop(shortCommand, shortCommand.length - maxLength / 2).join('');

        return `${start}...${end}`;
      }

      return shortCommand;
    },
  },
};
</script>

<style scoped>
</style>
