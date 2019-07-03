<template>
  <div class="settings">
    <h1><font-awesome-icon icon="cogs"></font-awesome-icon> Settings</h1>
    <!-- TODO: replace with messaging library (e.g. toastr) -->
    <div style="background: lightgreen;">
      {{ successDisplayMsg }}
    </div>
    <!-- TODO: warn when loaded from non-central server. Make read-only.-->
    <div v-if="!canEditSettings" style="background: salmon; padding: 4px;">
      These settings are read-only.
      <a v-if="centralServerSettingsUrl" :href="centralServerSettingsUrl">
        Click here to modify settings.
      </a>
    </div>
    <div style="background: salmon;">
      {{ failureDisplayMsg }}
    </div>
    <div>
      <h3>Auto-upgrade</h3>
      <div style="margin-left: 3em;">
      <select id="auto-upgrade" :value="selectedAutoUpgrade"
              @input="setAutoUpgrade($event.target.value)"
              :disabled="disableEdit">
        <option value="central">Central FireX Server</option>
        <option value="relative">Relative Flame Server</option>
        <option value="none">Disable Auto-Upgrade</option>
      </select>
      </div>
    </div>
    <div>
      <h3>Display Configuration</h3>

      <div style="margin-left: 3em;">
      <button type="button"
              style="margin-bottom: 10px;"
              v-clipboard:copy="displayConfigsJson"
              v-clipboard:success="onCopySuccess"
              v-clipboard:error="onCopyFail"
        >Copy Display Configs</button>

      <table class="display-config">
        <thead>
          <tr>
            <td>Service Name</td>
            <td>Operation</td>
            <td>Targets</td>
            <td>Actions</td>
          </tr>
        </thead>
        <tbody>
          <tr v-if="displayConfigs.length === 0" >
            <td colspan="4" style="padding: 20px;">
              No display operations have been saved. Create operations below.
            </td>
          </tr>
          <tr v-else v-for="config in displayConfigs" :key="config.id">
            <td>{{ config.relative_to_nodes.value }}</td>
            <td>{{ config.operation }}</td>
            <td>{{ join(config.targets) }}</td>
            <td style="text-align: center;">
              <font-awesome-icon icon="trash" style="cursor: pointer;" title="delete"
                @click="deleteDisplayConfig(config.id)"></font-awesome-icon>
            </td>
          </tr>
        </tbody>
      </table>

      <div style="margin-top: 15px;">
        <input type="checkbox" id="isNameRegex" v-model="inputDisplayConfig.isNameRegex"
          :disabled="disableEdit">
        <label for="isNameRegex" style="margin-right: 4px;">Regex</label>
        <input type="text" placeholder="Service Name" v-model="inputDisplayConfig.serviceName"
          style="margin-right: 10px" :disabled="disableEdit">
        <select style="margin: 10px;" v-model="inputDisplayConfig.operation"
                :disabled="disableEdit">
          <option value="collapse">collapse</option>
          <option value="expand">expand</option>
        </select>

        <div style="display: inline; padding: 10px;">
          <div style="display: inline; padding: 3px;">
            <input type="checkbox" id="self" value="self"
                   v-model="inputDisplayConfig.targets" :disabled="disableEdit">
            <label for="self">Self</label>
          </div>

          <div style="display: inline; padding: 3px;">
            <input type="checkbox" id="descendants" value="descendants"
                   v-model="inputDisplayConfig.targets" :disabled="disableEdit">
            <label for="descendants">Descendants</label>
          </div>

          <div style="display: inline;">
            <input type="checkbox" id="grandchildren" value="grandchildren"
              v-model="inputDisplayConfig.targets" :disabled="disableEdit">
            <label for="grandchildren">Grandchildren</label>
          </div>

          <div style="display: inline; padding: 3px;">
            <input type="checkbox" id="ancestors" value="ancestors"
                   v-model="inputDisplayConfig.targets" :disabled="disableEdit">
            <label for="ancestors">Ancestors</label>
          </div>
        </div>
      </div>
      <!--TODO: use validation library -->
      <div style="background: salmon">
        {{displayConfigError}}
      </div>

      <button @click="saveDisplayConfigEntry" :disabled="disableEdit">Save Display Config</button>
      </div>

    </div>

  </div>
</template>

<script>
import _ from 'lodash';
import { mapState } from 'vuex';

import { uuidv4 } from '../utils';
import {
  addLocalStorageData, readValidatedPathFromLocalStorage, loadDisplayConfigs, USER_CONFIGS_KEY,
} from '../persistance';

export default {
  name: 'XSettings',
  props: {
    inputFlameServer: { required: false, type: String },
  },
  data() {
    const autoUpgradeKey = 'autoFlameUpgrade';
    return {
      successDisplayMsg: '',
      autoUpgradeKey,
      selectedAutoUpgrade: this.readAutoUpgradeFromLocalStorage(autoUpgradeKey),
      displayConfigs: loadDisplayConfigs(),
      inputDisplayConfig: this.createEmptyDisplayConfigEntry(),
      displayConfigError: '',
      failureDisplayMsg: '',
    };
  },
  asyncComputed: {
    canEditSettings:
      {
        get() {
          const baseUrl = new URL(window.location.pathname, window.location.origin);
          // FIXME: use the field from the UI config instead of this.
          const fileMarkingCentralServer = new URL('send-firex-user-config.html', baseUrl);
          return fetch(fileMarkingCentralServer).then(r => r.ok, () => false);
        },
        default: false,
      },
  },
  computed: {
    ...mapState({
      centralServerUiPath: state => state.firexRunMetadata.centralServerUiPath,
      centralServer: state => state.firexRunMetadata.centralServer,
    }),
    displayConfigsJson() {
      return JSON.stringify(this.displayConfigs, null, 2);
    },
    disableEdit() {
      return !this.canEditSettings;
    },
    centralServerSettingsUrl() {
      if (!this.centralServer || !this.centralServerUiPath) {
        return null;
      }
      const settingsUrl = new URL(`${this.centralServerUiPath}#/settings`, this.centralServer);
      return settingsUrl.toString();
    },
  },
  methods: {
    addUserConfigs(newData) {
      addLocalStorageData(USER_CONFIGS_KEY, newData);
    },
    setAutoUpgrade(selectedValue) {
      this.selectedAutoUpgrade = selectedValue;
      this.addUserConfigs({ [[this.autoUpgradeKey]]: selectedValue });
      const displayString = {
        central: 'enabled central Flame',
        relative: 'enabled relative Flame',
        none: 'disabled',
      }[selectedValue];

      this.successDisplayMsg = `Successfully ${displayString} auto-upgrade.`;
    },
    readAutoUpgradeFromLocalStorage(autoUpgradeKey) {
      const validator = value => _.includes(['central', 'relative', 'none'], value);
      const defaultAutoUpgrade = 'relative';
      return readValidatedPathFromLocalStorage(
        USER_CONFIGS_KEY, autoUpgradeKey, validator, defaultAutoUpgrade,
      );
    },
    createEmptyDisplayConfigEntry() {
      return {
        serviceName: '',
        operation: 'collapse',
        targets: [],
        isNameRegex: false,
      };
    },
    saveDisplayConfigEntry() {
      this.displayConfigError = '';
      if (_.isEmpty(this.inputDisplayConfig.serviceName.trim())) {
        this.displayConfigError = 'Service name cannot be empty';
        return;
      }
      if (_.isEmpty(this.inputDisplayConfig.targets)) {
        this.displayConfigError = 'At least one target must be selected.';
        return;
      }
      // TODO: validate service if it's a regex.

      const newEntry = {
        id: uuidv4(),
        relative_to_nodes: {
          type: this.inputDisplayConfig.isNameRegex ? 'task_name_regex' : 'task_name',
          value: this.inputDisplayConfig.serviceName,
        },
        operation: this.inputDisplayConfig.operation,
        targets: this.inputDisplayConfig.targets,
      };
      this.displayConfigs.push(newEntry);
      this.inputDisplayConfig = this.createEmptyDisplayConfigEntry();
      this.addUserConfigs({ displayConfigs: this.displayConfigs });
    },
    deleteDisplayConfig(id) {
      this.displayConfigs = _.filter(this.displayConfigs, c => c.id !== id);
      this.addUserConfigs({ displayConfigs: this.displayConfigs });
    },
    join(array) {
      return _.join(array, ', ');
    },
    onCopySuccess() {
      this.successDisplayMsg = 'Successfully copied display config to clipboard.';
    },
    onCopyFail() {
      this.failureDisplayMsg = 'Failed to copy to clipboard';
    },
  },
};
</script>

<style>

  .settings {
    font-family: 'Source Sans Pro',sans-serif;
    margin: 10px;
  }

  .display-config td {
    border: 1px solid #cbcbcb;
    border-spacing: 0;
    padding: 5px;
  }

  .display-config {
    border-spacing: 0;
  }

  .display-config thead {
    font-weight: bold;
  }

</style>
