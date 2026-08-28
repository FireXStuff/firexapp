<template>
  <popper trigger="hover" :options="{ placement: 'bottom' }"
          style="border-left: 1px solid #000; height: 100%;"
          :disabled="!Boolean(link.title)">
    <div class="popper header-popover">{{link.title}}</div>

    <div slot="reference"  class="flame-link" :class="link._class">
      <router-link v-if="link.to" :to="link.to">
        <font-awesome-icon v-if="link.icon" :icon="link.icon" fixed-width/>
        <template v-if="link.text">{{link.text}}</template>
      </router-link>
      <a v-else-if="link.href" :href="link.href">
        <font-awesome-icon v-if="link.icon" :icon="link.icon" fixed-width/>
        <template v-if="link.text">{{link.text}}</template>
      </a>
      <a v-else-if="link.on"
           v-on:click="link.on()"
           :style="link.toggleState ? 'color: #2B2;' : ''">
        <font-awesome-icon v-if="link.icon" :icon="link.icon" fixed-width/>
        <template v-if="link.text">{{link.text}}</template>
      </a>
    </div>
  </popper>
</template>

<script>
import Popper from 'vue-popperjs';
import 'vue-popperjs/dist/vue-popper.css';

export default {
  name: 'XHeaderButton',
  components: { Popper },
  props: {
    link: { required: true, type: Object },
  },
};
</script>

<style>

.flame-link {
  font-family: 'Source Sans Pro',sans-serif;
  vertical-align: top;
  text-align: center;
  text-decoration: none;
  padding: 0 8px;
  color: #000;
  border-radius: 0;
  font-size: 20px;
  justify-content: flex-end;
  height: 100%;
  display: flex;
  align-items: center;
}

.flame-link a {
  text-decoration: none;
  font-family: 'Source Sans Pro',sans-serif;
  cursor: pointer;
}

.flame-link a {
  color: #000;

}

.flame-link a:hover {
  color: #2980ff;
}

.header-popover {
  padding: 3px;
  font-size: 13px;
  line-height: 1em;
  border-color: black;
  border-width: 1px;
  border-radius: 0;
  box-shadow: 2px 1px 1px rgb(58, 58, 58);
  font-family: 'Source Sans Pro',sans-serif;
}

.flame-link.kill-button{
  background-color: #dc3545!important;
}

.flame-link.kill-button a {
  color: #fff;
}

.kill-button:hover a {
  color: #2980ff;
}

</style>
