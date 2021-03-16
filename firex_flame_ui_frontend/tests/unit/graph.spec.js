import _ from 'lodash';
import { mount, createLocalVue } from '@vue/test-utils';
import VueRouter from 'vue-router';
import Vuex from 'vuex';
import fetchMock from 'jest-fetch-mock';
import { sync } from 'vuex-router-sync';

import router from '@/router/index';
import XGraph from '@/components/XGraph.vue';
import XCollapseableTaskNode from '@/components/nodes/XCollapsableTaskNode.vue';
import XSizeCapturingNodes from '@/components/nodes/XSizeCapturingNodes.vue';
import XCoreTaskNode from '@/components/nodes/XCoreTaskNode.vue';
import { defaultStoreOptions } from '../../src/store';

// Mock for Vue unit tests.
// eslint-disable-next-line no-global-assign
window = {
  URL: {
    createObjectURL: () => {},
  },
};

fetchMock.enableMocks();
const localVue = createLocalVue();
localVue.use(VueRouter);
localVue.use(Vuex);

const SIMPLE_TWO_NODE_CHAIN_BY_UUID = {
  1: {
    uuid: '1',
    name: 'rootName',
    parent_id: null,
  },
  2: {
    uuid: '2',
    name: 'childName',
    parent_id: '1',
  },
};

function transitionStub() {
  return {
    render() {
      // eslint-disable-next-line no-underscore-dangle
      return this.$options._renderChildren;
    },
  };
}

function fakeDimensionsByUuid(uuids) {
  return _.mapValues(_.keyBy(uuids), () => ({ width: 1, height: 2 }));
}

function getCleanFlameStore() {
  const clonedStoreOptions = _.cloneDeep(defaultStoreOptions());
  _.each(clonedStoreOptions.modules, (m, name) => {
    // modules must be deep cloned specifically. They do contain state.
    clonedStoreOptions[name] = _.cloneDeep(m);
  });
  return new Vuex.Store(clonedStoreOptions);
}

function createCollapseFlameData(uuid, targets) {
  return {
    _default_display: {
      order: 0,
      type: 'object',
      value: [
        {
          operation: 'collapse',
          relative_to_nodes: {
            type: 'task_uuid',
            value: uuid,
          },
          source_node: {
            type: 'task_uuid',
            value: uuid,
          },
          targets,
        },
      ],
    },
  };
}

function initGraphWrapper(tasksByUuid, store) {
  const runMetadata = { uid: 'FireX-user-xxxxx', root_uuid: '1' };
  store.commit('firexRunMetadata/setFlameRunMetadata', runMetadata);

  store.dispatch('tasks/setTasks', tasksByUuid);
  // Need to fake dimensions typically provided via the browser from getBoundingClientRect().
  store.dispatch('tasks/addTaskNodeSize', fakeDimensionsByUuid(
    _.keys(tasksByUuid),
  ));

  return mount(XGraph, {
    stubs: {
      'font-awesome-icon': true,
      transition: transitionStub(),
    },
    localVue,
    router,
    store,
  });
}

describe('XGraph.vue', () => {
  let store;

  beforeEach(() => {
    fetch.resetMocks();
    fetchMock.mockIf('/flame-ui-config.json', () => {});
    store = getCleanFlameStore();
    sync(store, router);
  });

  it('renders simple tree', async () => {
    const wrapper = initGraphWrapper(SIMPLE_TWO_NODE_CHAIN_BY_UUID, store);

    // console.log(wrapper.html());

    // Make sure 2 nodes in visible tree exist
    expect(wrapper.findAllComponents(XCollapseableTaskNode)).toHaveLength(2);

    // Make sure two invisible nodes created for size-tracking exist but are not visible.
    const sizeCapturingComp = wrapper.findComponent(XSizeCapturingNodes);
    expect(sizeCapturingComp.attributes().style).toContain('z-index: -10');
    const sizeCapturingTaskNodes = sizeCapturingComp.findAllComponents(XCoreTaskNode);
    expect(sizeCapturingTaskNodes).toHaveLength(2);
  });

  it('updates a simple tree with a new task', async () => {
    const wrapper = initGraphWrapper(SIMPLE_TWO_NODE_CHAIN_BY_UUID, store);

    store.dispatch('tasks/addTasksData', {
      3: {
        uuid: '3',
        name: 'childChildName',
        parent_id: '2',
      },
    });
    store.dispatch('tasks/addTaskNodeSize', fakeDimensionsByUuid(['3']));

    await wrapper.vm.$nextTick();
    expect(wrapper.findAllComponents(XCollapseableTaskNode)).toHaveLength(3);
  });

  it('updates a graph with collapsed tasks', async () => {
    const wrapper = initGraphWrapper(SIMPLE_TWO_NODE_CHAIN_BY_UUID, store);

    store.dispatch('tasks/addTasksData', {
      3: {
        uuid: '3',
        name: 'childChildName',
        parent_id: '2',
        flame_data: createCollapseFlameData('3', ['self', 'descendants']),
      },
      4: {
        uuid: '4',
        name: 'childChildChildName',
        parent_id: '3',
      },
    });
    store.dispatch('tasks/addTaskNodeSize', fakeDimensionsByUuid(['3', '4']));
    await wrapper.vm.$nextTick();

    // Expecting 2 collapsed nodes, 2 uncollapsed.
    const collapseableTaskNodes = wrapper.findAllComponents(XCollapseableTaskNode);
    expect(collapseableTaskNodes).toHaveLength(2);
    expect(collapseableTaskNodes.at(1).props().taskUuid).toBe('2');
    expect(collapseableTaskNodes.at(1).html()).toContain('2 Tasks');

    // 4 total size capturing nodes.
    expect(wrapper.findComponent(XSizeCapturingNodes).findAllComponents(XCoreTaskNode))
      .toHaveLength(2);
  });
});
