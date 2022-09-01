<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="headerParams.title"
              :main-title="headerParams.mainTitle"
              :links="headerParams.links"></x-header>

    <div class="node-attributes">

      <div class="node-container">
        <div v-if="parentWithAdditionals" class="tasks-outer-container">
          <div class="tasks-inner-container">
            <x-task-node v-for="parentUuid in parentWithAdditionals" :key="parentUuid"
                         :taskUuid="parentUuid" :allowCollapse="false" :isLeaf="false"
                         :showFlameData="false" class="other-task"></x-task-node>
          </div>
        </div>

        <x-task-node :taskUuid="uuid" :allowCollapse="false" :allowClickToAttributes="false"
          :isLeaf="false" style="padding: 0.5em;">
        </x-task-node>

        <div v-if="childrenUuids" class="tasks-outer-container">
          <div class="tasks-inner-container">
            <x-task-node v-for="childUuid in childrenUuids" :key="childUuid"
                         :taskUuid="childUuid" :allowCollapse="false" :isLeaf="false"
                         :showFlameData="false" class="other-task"></x-task-node>
          </div>
        </div>
      </div>

      <div style="display: inline-flex; align-items: center; justify-content: center;">

        <div style="padding: 1em;">

          <router-link
            v-if="detailedTask.cached_result_from"
            :to="getTaskRoute(detailedTask.cached_result_from)"
            class="btn btn-primary btn-lg"
            style="margin-right: 1em;"
            title="Click to view the task that initially produced these cached results."
          >
            <font-awesome-icon icon="briefcase"/>
            Cached Result From Task
          </router-link>

          <router-link v-if="detailedTask.exception_cause_uuid"
                       :to="getTaskRoute(detailedTask.exception_cause_uuid)"
                       class="btn btn-danger btn-lg" style="margin-right: 1em;">
            <font-awesome-icon icon="exclamation-circle"/>
            Causing Failure Task
          </router-link>

          <a class="btn btn-primary btn-lg" :href="detailedTask.logs_url" role="button">
            <font-awesome-icon icon="file-alt"/>
            {{detailedTask.name}} Log
          </a>
        </div>
        <div class="btn-group" role="group" style="">
          <button class="btn btn-primary" :class="{'active': selectedSection === 'all'}"
            @click="replaceSection('all')">
            All
          </button>
          <button v-if="hasFailure" class="btn btn-danger"
                  :class="{'active': selectedSection === 'failure'}"
                  @click="replaceSection('failure')">
            Failure
          </button>
          <button v-if="hasArguments" class="btn btn-primary"
                  :class="{'active': selectedSection === 'arguments'}"
                  @click="replaceSection('arguments')">
            Arguments
          </button>
          <button v-if="hasResults" class="btn btn-primary"
                  :class="{'active': selectedSection === 'results'}"
                  @click="replaceSection('results')">
            Results
          </button>
          <button v-if="hasExternalCommands"
                  class="btn btn-primary"
                  :class="{'active': selectedSection === 'external_commands'}"
                  @click="replaceSection('external_commands')">
            <font-awesome-icon v-if="someExternalCommandRunning && isRunstateIncomplete"
                               icon="circle-notch" class="fa-spin">
            </font-awesome-icon>
            External Commands
          </button>
          <button class="btn btn-primary"
                  :class="{'active': selectedSection === 'attributes'}"
                  @click="replaceSection('attributes')">
            Attributes
          </button>
        </div>
      </div>

      <x-section v-if="shouldShowSection('failure')" heading="Failure">
        <label class="node-attributes-label">traceback:</label>
        <div v-if="displayNode.traceback" style="display: inline;">
          <x-expandable-content button-class="btn-outline-danger" name="traceback"
                                :expand="expandAll || selectedSection === 'failure'">
              <pre style="overflow: auto; margin-top: 0" class="text-danger"
                   v-html="createLinkedHtml(displayNode.traceback.trim())"></pre>
          </x-expandable-content>
        </div>
        <label class="node-attributes-label">exception:</label>
        <div v-if="displayNode.exception" style="display: inline; color: darkred; margin-left: 1em;"
          v-html="createLinkedHtml(displayNode.exception.trim())">
        </div>
      </x-section>

      <x-section v-if="shouldShowSection('arguments')" heading="Arguments">
        <x-key-value-viewer :key-values="displayArguments"
                            :ordered-keys="displayArgumentKeys"
                            :expandAll="expandAll">
        </x-key-value-viewer>
      </x-section>

      <x-section v-if="shouldShowSection('results')" heading="Results">
        <x-key-value-viewer :key-values="displayResults" :expandAll="expandAll">
        </x-key-value-viewer>
      </x-section>

      <x-section v-if="shouldShowSection('external_commands')" heading="External Commands">
        <x-external-commands
          :external-commands="externalCommands"
          :taskLogsUrl="detailedTask.logs_url"
          :parentTask="detailedTask"/>
      </x-section>

      <x-section v-if="shouldShowSection('attributes')" heading="Attributes">
        <x-key-value-viewer :key-values="displayAttributes" :link-keys="['support_location']">
        </x-key-value-viewer>
      </x-section>
      </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { DateTime } from 'luxon';
import { mapGetters, mapState } from 'vuex';

import * as api from '../../api';
import {
  eventHub, isTaskStateIncomplete, durationString,
} from '../../utils';
import XHeader from '../XHeader.vue';
import XTaskNode from './XTaskNode.vue';
import XExpandableContent from '../XExpandableContent.vue';
import XExternalCommands from '../XExternalCommands.vue';
import XSection from '../XSection.vue';
import XKeyValueViewer from '../XKeyValueViewer.vue';

export default {
  name: 'XNodeAttributes',
  components: {
    XKeyValueViewer,
    XSection,
    XExternalCommands,
    XExpandableContent,
    XHeader,
    XTaskNode,
  },
  props: {
    uuid: { required: true, type: String },
    selectedSection: { default: 'all' },
    selectedSubsection: { default: null },
  },
  data() {
    return {
      showAllAttributes: false,
      taskAttributes: {},
    };
  },
  computed: {
    ...mapGetters({
      runDuration: 'tasks/runDuration',
      getTaskRoute: 'header/getTaskRoute',
      taskNameByUuid: 'tasks/taskNameByUuid',
      createLinkedHtml: 'header/createLinkedHtml',
      graphViewHeaderEntry: 'header/graphViewHeaderEntry',
    }),
    ...mapState({
      logsDir: state => state.firexRunMetadata.logs_dir,
      firexBin: state => _.get(state.header.uiConfig, 'firex_bin', 'firex'),
      uid: state => state.firexRunMetadata.uid,
    }),
    simpleTask() {
      return this.$store.getters['tasks/runStateByUuid'][this.uuid];
    },
    childrenUuids() {
      return this.$store.getters['graph/childrenAndAdditionalUuidsByUuid'][this.uuid];
    },
    taskChildren() {
      return _.map(this.childrenUuids,
        uuid => ({ uuid, name: this.taskNameByUuid[uuid] }));
    },
    parentWithAdditionals() {
      return this.$store.getters['graph/parentAndAdditionalUuidsByUuid'][this.uuid];
    },
    realParentUuid() {
      return this.$store.getters['graph/parentUuidByUuid'][this.uuid];
    },
    taskParent() {
      let parent;
      if (!_.isNil(this.realParentUuid)) {
        parent = {
          uuid: this.realParentUuid,
          name: this.taskNameByUuid[this.realParentUuid],
        };
      } else {
        parent = {};
      }
      return parent;
    },
    externalCommands() {
      return _.get(this.detailedTask, 'external_commands', {});
    },
    detailedTask() {
      const addedFields = { children: this.taskChildren, parent: this.taskParent };
      return _.assign(addedFields, this.taskAttributes);
    },
    displayNode() {
      // If we haven't fetched the details for some reason, just show the base properties.
      const task = _.pickBy(_.merge({}, this.simpleTask, this.detailedTask),
        v => !_.isObject(v) || !_.isEmpty(v));

      if (this.showAllAttributes) {
        task.minPriorityCollapseOp = this.minPriorityOp;
        _.each(task.states, (state, i) => {
          if (_.has(task.states, i + 1)) {
            const nextState = task.states[i + 1];
            const stateDuration = nextState.timestamp - state.timestamp;
            state.duration = durationString(stateDuration);
            state['% of task'] = 100 * stateDuration / task.actual_runtime;
            state['% of run'] = 100 * stateDuration / this.runDuration;
          }
        });
      }

      if (task.actual_runtime) {
        task.runtime = durationString(task.actual_runtime);
      }

      if (task.first_started) {
        task.first_started = this.formatTime(task.first_started);
      }

      return task;
    },
    supportLocation() {
      const subject = `[${this.uid}]: support request for ${this.detailedTask.name}`;
      const nl = '%0D%0A';
      const body = `${nl + nl + nl}task: ${window.location.toString() + nl}`
        + `full name: ${this.detailedTask.long_name + nl}`;
      return `${this.detailedTask.support_location}?subject=${subject}&body=${body}`;
    },
    isRunstateIncomplete() {
      return isTaskStateIncomplete(this.simpleTask.state);
    },
    headerParams() {
      let links = [
        {
          name: 'showAllAttributes',
          on: () => {
            // eslint-disable-next-line
            this.showAllAttributes = !this.showAllAttributes;
          },
          toggleState: this.showAllAttributes,
          icon: 'plus-circle',
          title: 'Show All Attributes',
        },
      ];
      if (this.detailedTask.code_url) {
        // TODO: use new 'code_filepath' & central_server instead of 'code_url'
        links.push(
          {
            name: 'code',
            href: this.detailedTask.code_url,
            icon: 'file-code',
            title: 'See Code',
          },
        );
      }
      links = links.concat(
        [
          this.graphViewHeaderEntry,
          // TODO: use new 'log_filepath' field & central_server instead of 'logs_url'.
          {
            name: 'logs',
            href: this.detailedTask.logs_url,
            text: 'Task Log',
            icon: 'file-alt',
          },
          {
            name: 'support',
            href: this.supportLocation,
            text: 'Support',
            icon: 'question-circle',
          },
        ],
      );

      if (this.isRunstateIncomplete) {
        // Add revoke button after all other links.
        links.push({
          name: 'kill',
          on: () => eventHub.$emit('revoke-task', this.uuid),
          _class: 'kill-button',
          icon: 'times',
          title: 'Kill This Task',
        });
      }

      return {
        title: this.uid,
        mainTitle: this.detailedTask.long_name,
        links,
      };
    },
    minPriorityOp() {
      return this.$store.getters['graph/resolvedCollapseStateByUuid'][this.uuid].minPriorityOp;
    },
    hasFailure() {
      return this.detailedTask.exception || this.detailedTask.traceback;
    },
    hasArguments() {
      return !_.isEmpty(this.defaultArguments) || !_.isEmpty(this.nonDefaultArguments);
    },
    hasResults() {
      return !_.isNil(this.detailedTask.firex_result);
    },
    hasExternalCommands() {
      return Boolean(Object.keys(this.externalCommands).length);
    },
    expandAll() {
      return this.showAllAttributes;
    },
    defaultArguments() {
      return _.get(this.displayNode, 'firex_default_bound_args', {});
    },
    nonDefaultArguments() {
      return _.get(this.displayNode, 'firex_bound_args', {});
    },
    displayArgumentKeys() {
      return _.concat(_.map(_.sortBy(_.keys(this.defaultArguments)), k => `(default) ${k}`),
        _.sortBy(_.keys(this.nonDefaultArguments)));
    },
    displayArguments() {
      return Object.assign({}, _.mapKeys(this.defaultArguments, (v, k) => `(default) ${k}`),
        this.nonDefaultArguments);
    },
    displayResults() {
      const taskResult = _.get(this.displayNode, 'firex_result', {});
      if (!_.isObject(taskResult)) {
        if (_.isNull(taskResult)) {
          // Deliberately show nothing when return value is None, since this is default return
          // value.
          return {};
        }
        return { task_result: taskResult };
      }
      return taskResult;
    },
    displayAttributes() {
      if (this.showAllAttributes) {
        return _.omit(this.displayNode,
          ['firex_bound_args', 'firex_default_bound_args', 'exception', 'traceback',
            'firex_result']);
      }
      return _.pick(this.displayNode,
        ['first_started', 'hostname', 'runtime', 'support_location', 'utcoffset', 'uuid']);
    },
    someExternalCommandRunning() {
      return _.some(this.externalCommands, c => !_.has(c, 'result'));
    },
  },
  methods: {
    fetchTaskAttributes() {
      this.taskAttributes = {};
      api.fetchTaskDetails(this.uuid).then((taskAttributes) => {
        this.taskAttributes = taskAttributes;
        this.$nextTick(this.scrollToSelectedSubsection);
      });
    },
    formatTime(unixTime) {
      const humanTime = DateTime.fromSeconds(unixTime)
        .toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
      return `${humanTime} (orig: ${unixTime})`;
    },
    shouldShowSection(sectionId) {
      if (this.selectedSection === 'all' || this.selectedSection === sectionId) {
        const idToCheckPresent = {
          failure: this.hasFailure,
          arguments: this.hasArguments,
          results: this.hasResults,
          external_commands: this.hasExternalCommands,
          attributes: true,
          replay: true,
        };
        return _.get(idToCheckPresent, sectionId, false);
      }
      return false;
    },
    replaceSection(section) {
      this.$router.replace(this.getTaskRoute(this.uuid, section));
    },
    scrollToSelectedSubsection() {
      if (this.selectedSubsection) {
        const e = this.$el.querySelector(`#subsection${this.selectedSubsection}`);
        if (e) {
          e.scrollIntoView(true);
        }
      }
    },
  },
  watch: {
    // eslint-disable-next-line
    '$route.params.uuid': {
      handler: 'fetchTaskAttributes',
      immediate: true,
    },
    '$route.params.selectedSubsection': {
      handler: 'scrollToSelectedSubsection',
      // immediate case needs to be handled in fetchTaskAttributes so that $el and
      // task details are defined.
      immediate: false,
    },
  },
};
</script>

<style scoped>

.node-attributes-label {
  font-weight: 700;
}

.node-attributes {
  font-family: 'Source Code Pro', monospace;
  font-size: 14px;
  margin: 10px;
  flex: 1;
  overflow: auto;
  display: flex;
  flex-direction: column;
}

.node-container {
  display: flex;
  align-items: center;
  flex-direction: column;
}

.other-task {
  transform: scale(0.66);
  opacity: 0.4;
  transition: all .2s ease-in-out;
}

.other-task:hover {
  transform: scale(1);
  opacity: 1;
}

.tasks-outer-container {
  display: flex;
  width: 95%;
  justify-content: center;
}

.tasks-inner-container {
  display: flex;
  overflow-x: auto;
  overflow-y: visible;
  justify-content: space-between;
}

</style>
