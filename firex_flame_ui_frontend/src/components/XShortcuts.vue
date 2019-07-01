<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="headerParams.title"
              :links="links"
              :legacyPath="headerParams.legacyPath"></x-header>

    <div class="help-content">
      <h1>General</h1>
      <ul class="shortcuts">
        <li>c → center the task chart</li>
        <li>r → refresh the task chart</li>
        <li>u → toggle auto update</li>
        <li>d → toggle task details in the tree view</li>
      </ul>
      <h1>Searching</h1>
      <ul class="shortcuts">
        <li>/ <i>or</i> [CTRL] + f → enter into search mode</li>
        <li>[ENTER] → run first search/continue through results</li>
        <li>[SHIFT] + [ENTER] → reverse the search direction</li>
        <!--<li>[SPACE] → go to the current task's page</li>-->
        <!--<li>[ALT] → set the current task as root</li>-->
        <li>[ESC] → exit the search mode</li>
      </ul>
      <h1>Chart Manipulation</h1>
      <ul class="shortcuts">
        <li>[SHIFT] + [LEFT MOUSE CLICK] on task → make selected task the "root"</li>
      </ul>
    </div>

  </div>
</template>

<script>
import { mapGetters } from 'vuex';

import XHeader from './XHeader.vue';

export default {
  name: 'XShortcuts',
  components: { XHeader },
  data() {
    return {
      headerParams: {
        title: 'Keyboard Shortcuts',
        legacyPath: '/shortcuts',
      },
    };
  },
  computed: {
    ...mapGetters({
      documentationHeaderEntry: 'header/documentationHeaderEntry',
      helpViewHeaderEntry: 'header/helpViewHeaderEntry',
    }),
    links() {
      return [this.documentationHeaderEntry, this.helpViewHeaderEntry];
    },
  },
};
</script>

<style scoped>

h1 {
  font-family: 'Source Sans Pro', sans-serif;
  font-size: 1.4em;
  margin-bottom: 0;
}

.help-content {
  font-family: 'Source Sans Pro', sans-serif;
  margin: 10px;
  font-size: 1em;
}

.shortcuts {
  font-size: 1.2em;
  column-count: 2;
}

ul {
  text-align: center;
  padding-left: 0;
  margin: 0;
  list-style: none;
}

.shortcuts li {
  width: 100%;
  border: 0;
  display: inline-block;
  text-align: left;
}

ul li {
  padding: 12px;
  max-width: 540px;
  text-align: center;
  display: inline-table;
  border-left: 1px solid #000;
}
</style>
