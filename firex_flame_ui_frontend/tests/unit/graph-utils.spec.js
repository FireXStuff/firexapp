import _ from 'lodash';

import { getCollapsedGraphByNodeUuid } from '@/utils.js';
import { getGraphDataByUuid } from '@/graph-utils.js';
import { resolveCollapseStatusByUuid } from '@/collapse.js';

describe('utils.js', () => {
  const trivRoot = 1;
  const trivialGraph = getGraphDataByUuid(trivRoot, {
    1: null,
    2: '1',
    3: '2',
  });

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
    const graphDataByUuid = getGraphDataByUuid('1', { '1': null });
    expect(graphDataByUuid['1'].parentId).toBe(null);
    expect(graphDataByUuid['1'].childrenUuids).toEqual([]);
    expect(graphDataByUuid['1'].descendantUuids).toEqual([]);
    expect(graphDataByUuid['1'].ancestorUuids).toEqual([]);
  });
});
