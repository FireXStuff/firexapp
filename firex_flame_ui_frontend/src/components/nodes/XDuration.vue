<template>
  <div>{{duration}}</div>
</template>

<script>
import { mapState } from 'vuex';
import {
  durationString, isTaskStateIncomplete,
} from '../../utils';

/**
 * Separate, trivial component to avoid peer checks on live-update rerender.
 */
export default {
  name: 'XDuration',
  props: {
    runState: { required: true },
    firstStarted: { required: true },
    actualRuntime: { required: true },
  },
  data() {
    return {
      // Updated locally if the node is incomplete, otherwise the actual_runtime value is shown.
      liveRunTime: 0,
    };
  },
  created() {
    this.updateLiveRuntimeAndSchedule();
  },
  computed: {
    ...mapState({
      liveUpdate: state => state.graph.liveUpdate,
    }),
    duration() {
      let runtime;
      if (!isTaskStateIncomplete(this.runState) && this.actualRuntime) {
        runtime = this.actualRuntime;
      } else if (!runtime && this.firstStarted) {
        runtime = this.liveRunTime;
      } else {
        return '';
      }
      return `time: ${durationString(runtime)}`;
    },
  },
  methods: {
    updateLiveRuntimeAndSchedule() {
      if (this.liveUpdate
        && (isTaskStateIncomplete(this.runState))
        && this.firstStarted) {
        this.liveRunTime = (Date.now() / 1000) - this.firstStarted;
        // TODO: could reduce total number of re-renders due to duration update by having
        // a central timeout that updates all incomplete durations (instead of timeout
        // per-incomplete task).
        setTimeout(() => {
          // Note liveUpdate may have changed since this timeout was set, so double check.
          if (this.liveUpdate) {
            this.updateLiveRuntimeAndSchedule();
          }
        }, 3000);
      }
    },
  },
};
</script>

<style scoped>
</style>
