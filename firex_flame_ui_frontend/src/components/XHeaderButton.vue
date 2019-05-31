<template>
    <div class="flame-link">
      <router-link v-if="link.to" :to="link.to">
        <font-awesome-icon v-if="link.icon" :icon="link.icon" fixed-width/>
        <template v-if="link.text">{{link.text}}</template>
      </router-link>
      <a v-else-if="link.href" :href="link.href">
        <font-awesome-icon v-if="link.icon" :icon="link.icon" fixed-width/>
        <template v-if="link.text">{{link.text}}</template>
      </a>
      <div v-else-if="link.on"
           class="header-icon-button"
           v-on:click="link.on()"
           :class="link._class"
           :style="link.toggleState ? 'color: #2B2;' : ''">
        <font-awesome-icon v-if="link.icon" :icon="link.icon" fixed-width/>
        <template v-if="link.text">{{link.text}}</template>
      </div>
    </div>
</template>

<script>

export default {
  name: 'XHeaderButton',
  props: {
    link: { required: true, type: Object },
  },
};
</script>

<style>

.flame-link {
  font-family: 'Source Sans Pro',sans-serif;
  vertical-align: top;
  border-left: 1px solid #000;
  line-height: 40px;
  text-align: center;
  text-decoration: none;
  padding: 0 8px;
  color: #000;
  border-radius: 0;
  font-size: 20px;
  justify-content: flex-end;
}

.flame-link a {
  text-decoration: none;
  font-family: 'Source Sans Pro',sans-serif;
}

.header-icon-button {
  justify-content: end;
  cursor: pointer;
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

</style>
