import router from '/src/router/index';

async function pushAndGetParams(path) {
  await router.push(path);
  await router.isReady();
  return router.currentRoute.value.params;
}

describe('router', () => {
  /*
   * vue-router 3 left unmatched optional params undefined, but vue-router 4 sets them to
   * ''. Several readers depend on the old semantics: XNodeAttributes' selectedSection prop
   * default, and isRequiredAccessModeDataPresent's isNil check on inputFireXId.
   */
  it('omits optional params that did not match', async () => {
    const params = await pushAndGetParams('/FireX-user-250914-120000-1/tasks/some-uuid');

    expect(params.uuid).toBe('some-uuid');
    expect(params.selectedSection).toBeUndefined();
    expect(params.selectedSubsection).toBeUndefined();
  });

  it('omits inputFireXId when no run is in the path', async () => {
    expect((await pushAndGetParams('/list')).inputFireXId).toBeUndefined();
  });

  it('keeps optional params that did match', async () => {
    const params = await pushAndGetParams(
      '/FireX-user-250914-120000-1/tasks/some-uuid/results',
    );

    expect(params.inputFireXId).toBe('FireX-user-250914-120000-1');
    expect(params.selectedSection).toBe('results');
  });
});
