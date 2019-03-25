<template>
  <div>
    <button v-on:click="toggleAutoUpgrade">{{changeMessage}}</button>
    <div v-if="displayMsg">
      {{displayMsg}}
    </div>
  </div>
</template>

<script>
import _ from 'lodash'

export default {
  name: 'HelloWorld',
  data () {
    let autoUpgradeKey = 'auto-flame-upgrade'
    return {
      displayMsg: '',
      autoUpgradeKey: autoUpgradeKey,
      isAutoUpgradeEnabled: localStorage.getItem(autoUpgradeKey) === 'true',
    }
  },
  computed: {
    changeMessage () {
      return (this.isAutoUpgradeEnabled ? 'Disable' : 'Enable') + ' Flame Auto-Upgrade'
    },
  },
  created () {
    if (_.isNull(localStorage.getItem(this.autoUpgradeKey))) {
      this.toggleAutoUpgrade()
    }
  },
  methods: {
    toggleAutoUpgrade () {
      this.isAutoUpgradeEnabled = !this.isAutoUpgradeEnabled
      let newVal = this.isAutoUpgradeEnabled ? 'true' : 'false'
      localStorage.setItem(this.autoUpgradeKey, newVal)
      let displayString = this.isAutoUpgradeEnabled ? 'enabled' : 'disabled'
      this.displayMsg = 'Successfully ' + displayString + ' flame auto-upgrade.'
    },
  },
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
</style>
