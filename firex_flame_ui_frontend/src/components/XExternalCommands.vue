<template>
  <div>
    <x-section v-if="displayExternalCommands.length > 1">
      <template v-slot:header><strong style="font-size: medium;">Summary</strong></template>
      <div>
        <router-link v-for="displayCommand in displayExternalCommands" :key="'summary-' + displayCommand.id"
                     :to="{params: { selectedSubsection: displayCommand.id }}">
          <div style="display: flex; flex-direction: row;">
            <div>
              <x-external-command-status
                :display-external-command="displayCommand"
                :parent-task-complete="parentTaskComplete"
                :show-text="false"/>
            </div>
            <div style="flex: 1;">{{summarizeCommand(displayCommand.cmd)}}</div>
            <div style="align-self: flex-end;">{{displayCommand.duration}}</div>
          </div>
        </router-link>
      </div>
    </x-section>

    <x-section
      v-for="displayCommand in displayExternalCommands"
      :key="displayCommand.id" style="margin-bottom: 3em;"
      :id="'subsection' + displayCommand.id">

      <template v-slot:header>
        <!-- v-if here breaks slot. -->
        <div
          v-show="displayCommand.remoteHost"
          style="display: inline; font-weight: bold;"
          >[ssh {{ displayCommand.remoteHost }}]
        </div>
        <div style="display: inline; font-style: italic;"
          >{{ displayCommand.cwd }}</div>
        <strong style="font-size: medium;">$
          <span v-html="createLinkedHtml(displayCommand.cmd)"/>

          <button v-if="isExpandableOutput(displayCommand)"
            class="btn btn-sm btn-primary pull-right" @click="expandedOutputIds.push(displayCommand.id)">
            <font-awesome-icon icon="expand-arrows-alt"></font-awesome-icon>
          </button>
          <router-link class="btn btn-link pull-right" :to="{params: { selectedSubsection: displayCommand.id }}">
            <font-awesome-icon icon="link"></font-awesome-icon>
          </router-link>
        </strong>
      </template>

      <div>
        <template v-if="displayCommand.result">
          <h4 v-if="displayCommand.stderrOutput" style="text-decoration: underline;">
            stdout
          </h4>
          <pre
              style="background-color: #fafafa;"
              :style="{color: displayCommand.result.returncode ? 'darkred' : '',
                      'max-height': isExpandedOutput(displayCommand.id) ? null : '15em'}"
              ref="outputs"
          >{{ displayCommand.output }}</pre>

          <template v-if="displayCommand.stderrOutput">
            <h4 style="text-decoration: underline;">stderr</h4>
            <pre
                style="background-color: #fafafa;"
                :style="{color: 'darkred',
                        'max-height': isExpandedOutput(displayCommand.id) ? null : '15em'}"
                ref="outputs"
            >{{ displayCommand.stderrOutput }}</pre>
          </template>

        </template>
        <template v-else-if="!parentTaskComplete">
          <router-link
            v-if="!displayCommand.remoteHost"
            :to="getLiveFileRoute(displayCommand.file, displayCommand.host)"
            class="btn btn-primary" style="margin: 1em 0;">
            <font-awesome-icon :icon="['far', 'eye']"/>
            View Live Output
          </router-link>
        </template>
        <div class="row" style="margin-left: 2em">
          <div class="col-md-2">
            <strong>time: </strong> {{ displayCommand.duration }}
          </div>

          <x-external-command-status
            :display-external-command="displayCommand"
            :parent-task-complete="parentTaskComplete"
          />

          <div class="col-md-4">
            <strong>started: </strong> {{ displayCommand.startTime }}
          </div>
          <div v-if="displayCommand.taskLogLink" class="col-md-4">
            <a :href="displayCommand.taskLogLink">
              <font-awesome-icon icon="file-alt"/>
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
    parentTask: { type: Object },
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
      return _.sortBy(
        _.mapValues(this.externalCommands, (c, id) => ({
          id,
          cmd: this.formatCommand(c.cmd),
          result: c.result,
          output: this.formatOutput(c),
          stderrOutput: this.formatStderrOutput(c),
          duration: this.commandDuration(c),
          startTimeEpoch: c.start_time,
          startTime: this.formatTime(c.start_time),
          taskLogLink: this.taskLogsUrl ? `${this.taskLogsUrl}#${id}` : null,
          host: c.host,
          file: c.output_file,
          cwd: c.cwd ? _.trim(c.cwd) : c.cwd,
          remoteHost: this.displayRemoteHost(_.get(c, 'remote_host', null)),
        })),
        'startTimeEpoch',
      );
    },
    parentTaskComplete() {
      return !_.isNull(this.parentTaskEndTime);
    },
    parentTaskEndTime() {
      if (!this.parentTask.actual_runtime) {
        return null;
      }
      return this.parentTask.first_started + this.parentTask.actual_runtime;
    },
    parentTaskHost() {
      return _.last(_.split(_.get(this.parentTask, 'hostname'), '@', 2));
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
      return DateTime.fromSeconds(unixTime).toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
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
    formatStderrOutput(cmd) {
      if (!_.has(cmd, 'result.stderr_output')) {
        return null;
      }
      const stderr = cmd.result.stderr_output;
      if (_.get(cmd, 'result.stderr_output_truncated')) {
        return `<truncated>\n...${stderr}`;
      }
      return stderr;
    },
    isExpandableOutput(displayCommand) {
      const newlinesCount = _.countBy(displayCommand.output)['\n'] || 0;
      const stderrNewLinesCount = _.countBy(displayCommand.stderrOutput || '')['\n'] || 0;
      const someOutputBig = newlinesCount > 6 || stderrNewLinesCount > 6;
      return someOutputBig && !this.isExpandedOutput(displayCommand.id);
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
    displayRemoteHost(remoteHost) {
      if (!remoteHost) {
        return remoteHost;
      }
      if (remoteHost === 'localhost' && this.parentTaskHost) {
        return this.parentTaskHost;
      }
      return remoteHost;
    },
  },
};
</script>

<style scoped>
</style>
