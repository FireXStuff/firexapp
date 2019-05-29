// TODO: why isn't lodash found when running from pycharm?
// import _ from 'lodash';
import { mount, createLocalVue } from '@vue/test-utils';
import VueRouter from 'vue-router';
import Vuex from 'vuex'

import XGraph from '@/components/XGraph.vue';
import store from '@/store';

const localVue = createLocalVue();
localVue.use(VueRouter);
localVue.use(Vuex);
const router = new VueRouter();

const simpleNodesByUuid = {
  1: {
    uuid: '1',
    name: 'rootName',
    parent_id: null,
  },
};


describe('XGraph.vue', () => {
  it('renders simple tree', () => {
    const wrapper = mount(XGraph, {
      propsData: {
        nodesByUuid: simpleNodesByUuid,
        runMetadata: { uid: 'FireX-user-xxxxx', root_uuid: '1' },
        liveUpdate: false,
      },
      localVue,
      router,
      store,
    });
    // console.log(wrapper.html());
    // expect(wrapper.text()).toMatch(msg);
  });
});
