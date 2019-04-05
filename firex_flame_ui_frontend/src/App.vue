<template>
  <div id="app">
    <router-view/>
  </div>
</template>

<script>
export default {
  name: 'App',
};
</script>

<style>

  body {
    margin: 0;
  }

  html, body {
    margin: 0;
    height: 100%; /* can also use viewport units (height: 100vh) */
  }
  #app { height: 100%;}
</style>
