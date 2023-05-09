<template>
  <div>{{duration}}</div>
</template>

<script>
import _ from 'lodash';
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
    approxRuntime: { required: true },
  },
  data() {
    const localLatestNow = Date.now() / 1000;
    return {
      // Updated liveRunTime locally if the node is incomplete, otherwise the actual_runtime value
      // is shown.
      // Note the server time might be ahead of local time (both in UTC), so we'll pin to zero
      // in this case then update locally.
      liveRunTime: _.max([localLatestNow - this.firstStarted, 0]),
      localLatestNow,
    };
  },
  computed: {
    ...mapState({
      liveUpdate: state => state.graph.liveUpdate,
    }),
    duration() {
      let runtimeStr;
      if (this.runState === 'task-incomplete' && _.isNil(this.actualRuntime)) {
        // UI-side run-state kludging, no idea what actual runtime is.
        let approxRuntimeStr;
        if (this.approxRuntime) {
          approxRuntimeStr = durationString(this.approxRuntime);
        } else {
          approxRuntimeStr = '';
        }
        runtimeStr = `${approxRuntimeStr}?`;
      } else {
        let runtime;
        if (!isTaskStateIncomplete(this.runState) && this.actualRuntime) {
          runtime = this.actualRuntime;
        } else if (this.firstStarted) {
          runtime = this.liveRunTime;
        } else {
          return '';
        }
        runtimeStr = durationString(runtime);
      }
      return `time: ${runtimeStr}`;
    },
  },
  methods: {
    updateLiveRuntimeAndSchedule() {
      if (this.liveUpdate
        && isTaskStateIncomplete(this.runState)
        && this.firstStarted) {
        const newLatestNow = Date.now() / 1000;
        this.liveRunTime += newLatestNow - this.localLatestNow;
        this.localLatestNow = newLatestNow;
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
  watch: {
    liveUpdate: {
      handler() {
        this.updateLiveRuntimeAndSchedule();
      },
      immediate: true,
    },
  },
};
</script>

<style scoped>
</style>
