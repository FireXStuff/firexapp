import _ from 'lodash';

import { getGraphDataByUuid } from '@/graph-utils.js';
import { resolveCollapseStatusByUuid, getCollapsedGraphByNodeUuid } from '@/collapse.js';

describe('utils.js', () => {
  const trivRoot = 1;
  const trivChainDepthByUuid = _.keyBy(['1', '2', '3'], () => 0);
  const trivialGraph = getGraphDataByUuid(trivRoot, {
    1: null,
    2: '1',
    3: '2',
  },
  null,
  trivChainDepthByUuid);

  it('collects children', () => {
    expect(trivialGraph[1].childrenUuids).toEqual(['2']);
    expect(trivialGraph[2].childrenUuids).toEqual(['3']);
    expect(trivialGraph[3].childrenUuids).toEqual([]);
  });

  it('collects descendants', () => {
    expect(trivialGraph[1].descendantUuids).toEqual(['2', '3']);
    expect(trivialGraph[2].descendantUuids).toEqual(['3']);
    expect(trivialGraph[3].descendantUuids).toEqual([]);
  });

  it('collects parentIds', () => {
    expect(trivialGraph[1].parentId).toBe(null);
    expect(trivialGraph[2].parentId).toEqual('1');
    expect(trivialGraph[3].parentId).toEqual('2');
  });

  it('handles single node graph', () => {
    const graphDataByUuid = getGraphDataByUuid('1', { 1: null }, null, { 1: 0 });
    expect(graphDataByUuid['1'].parentId).toBe(null);
    expect(graphDataByUuid['1'].childrenUuids).toEqual([]);
    expect(graphDataByUuid['1'].descendantUuids).toEqual([]);
    expect(graphDataByUuid['1'].unchainedAncestorUuids).toEqual([]);
  });
});
