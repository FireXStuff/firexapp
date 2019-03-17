<template>
  <div id="app">
    <router-view/>
  </div>
</template>

<script>
export default {
  name: 'App',
}
</script>

<style>

  body {
    margin: 0;
    /* Flex boxes should scroll, not the top-level. This fixes the header. Maybe should be done elsewhere*/
    /*overflow: hidden;*/
  }

  html, body {
    margin: 0;
    height: 100%; /* can also use viewport units (height: 100vh) */
  }
  #app { height: 100%;}
</style>
