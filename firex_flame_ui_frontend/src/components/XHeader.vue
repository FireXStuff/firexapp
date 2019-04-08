<template>
    <div class="header">
      <div style="display: flex; flex-direction: row;">
        <div>
          <router-link :to="{ name: 'XGraph',
            query: {logDir: $route.query.logDir, flameServer: $route.query.flameServer}}">
            <img style='height: 36px;' src="../assets/firex_logo.png" alt="firex logo">
          </router-link>
        </div>
        <div class="uid">{{title}}</div>

        <!-- TODO: Not great reading flame server directly from route -->
        <a :href="$route.query.flameServer + legacyPath + '?noUpgrade=true'"
           class="flame-link" style="font-size: 16px;">
          <font-awesome-icon icon="fire"></font-awesome-icon>
            Back to Legacy
          <font-awesome-icon icon="fire"></font-awesome-icon>
        </a>

        <div style="margin-left: auto; display: flex;">

          <!-- TODO: generalize this with a slot or something instead of hardcoding
                this component.-->
          <x-task-node-search v-if="enableSearch" class="header-icon-button"></x-task-node-search>

          <template v-for="link in links">
            <router-link v-if="link.to" class="flame-link" :to="link.to" :key="link.name">
              <font-awesome-icon v-if="link.icon" :icon="link.icon"></font-awesome-icon>
              <template v-if="link.text">{{link.text}}</template>
            </router-link>
            <a v-else-if="link.href" class="flame-link" :href="link.href" :key="link.name">
              <font-awesome-icon v-if="link.icon" :icon="link.icon"></font-awesome-icon>
              <template v-if="link.text">{{link.text}}</template>
            </a>
            <div v-else-if="link.on"
                 class="header-icon-button"
                 v-on:click="link.on()"
                 :class="link._class"
                 :style="link.toggleState ? 'color: #2B2;' : ''"
                 :key="link.name">
              <font-awesome-icon v-if="link.icon" :icon="link.icon"></font-awesome-icon>
              <template v-if="link.text">{{link.text}}</template>
            </div>
          </template>
        </div>
      </div>
    </div>
</template>

<script>
import XTaskNodeSearch from './XTaskNodeSearch.vue';

export default {
  name: 'XHeader',
  components: { XTaskNodeSearch },
  props: {
    title: { default: '' },
    links: { default: () => [], type: Array },
    legacyPath: { default: '' },
    enableSearch: { default: false },
  },
};
</script>

<style scoped>

.header {
  background-color: #EEE;
  border-bottom: 1px solid #000;
}

.uid {
  font-family: 'Source Sans Pro',sans-serif;
  margin: 0 8px;
  padding: 0;
  white-space: nowrap;
  font-size: 20px;
  line-height: 40px;
  font-weight: normal;
}

.flame-link {
  font-family: 'Source Sans Pro',sans-serif;
  vertical-align: top;
  border-left: 1px solid #000;
  line-height: 40px;
  text-align: center;
  padding: 0 8px;
  text-decoration: none;
  color: #000;
  border-radius: 0;
  font-size: 20px;
  justify-content: flex-end;
}

.header-icon-button {
  padding: 0 8px;
  border-left: 1px solid #000;
  justify-content: end;
  font-size: 20px;
  line-height: 40px;
  cursor: pointer;
  color: #000;
}

.header-icon-button:hover {
    color: #2980ff;
}

a {
  color: #000;
}

a:hover {
    color: #2980ff;
}

.kill-button {
  color: #900;
}

.kill-button:hover {
  color: #fff;
  background: #900;
}

</style>
