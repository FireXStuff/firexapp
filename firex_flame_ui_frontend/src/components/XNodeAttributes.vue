<template>
  <div>
  {{node.name}}
  </div>
</template>

<script>
/* eslint-disable */
import _ from 'lodash';

export default {
  name: 'XNodeAttributes',
  props: {
    node: {required: true, type: Object}
  },
  computed: {

  },
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>


.link {
  fill: none;
  stroke: #000; /*#ccc; */
  stroke-width: 2px; /*4px;*/
}

#chart-container {
    position: absolute;
    top: 40px; /* Header Height */
    bottom: 20px; /* Footer Height */
    width: 100%;
}

</style>
