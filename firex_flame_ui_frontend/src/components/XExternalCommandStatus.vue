<template>
  <!-- Compensate for when the end event is never received, even though the parent
            task is complete.-->
  <div v-if="!displayExternalCommand.result && parentTaskComplete" class="col-md-2 result-warning"
       title="Never received completion event, likely task was terminated.">
    <x-status-icon status="warning"></x-status-icon>
    <strong v-if="showText"> incomplete</strong>
  </div>
  <div v-else-if="!displayExternalCommand.result" class="col-md-2" style="color: #07d">
     <x-status-icon status="in_progress"></x-status-icon>
    <strong v-if="showText"> running</strong>
  </div>
  <div v-else-if="displayExternalCommand.result.timeout" class="col-md-2 result-warning">
    <x-status-icon status="warning"></x-status-icon>
    <strong v-if="showText"> completion timeout</strong>
  </div>
  <div v-else-if="displayExternalCommand.result.inactive" class="col-md-2 result-warning">
    <x-status-icon status="warning"></x-status-icon>
    <strong v-if="showText"> inactivity (no output) timeout</strong>
  </div>
  <div v-else-if="displayExternalCommand.result.returncode" class="col-md-2 result-failure">
    <x-status-icon status="failed"></x-status-icon>
    <template v-if="showText">
      <strong> returncode:</strong> {{displayExternalCommand.result.returncode}}
    </template>
  </div>
  <div v-else-if="displayExternalCommand.result.returncode === 0" class="col-md-2 result-success">
    <x-status-icon status="success"></x-status-icon>
    <strong v-if="showText"> success</strong>
  </div>
  <div v-else-if="!displayExternalCommand.result.completed" class="col-md-2 result-warning">
    <x-status-icon status="warning"></x-status-icon>
    <strong v-if="showText"> interrupted</strong>
  </div>
  <div v-else class="col-md-2 result-warning">
    <x-status-icon status="unknown"></x-status-icon>
    <strong v-if="showText"> status unknown</strong>
  </div>
</template>

<script>

import XStatusIcon from './XStatusIcon.vue';

export default {
  name: 'XExternalCommandStatus',
  components: { XStatusIcon },
  props: {
    displayExternalCommand: { required: true, type: Object },
    showText: { default: true, type: Boolean },
    parentTaskComplete: { required: true, type: Boolean },
  },
};
</script>

<style scoped>
  .result-warning {
    color: darkorange;
  }

  .result-failure {
    color: darkred;
  }

  .result-success {
    color: green;
  }
</style>
