# FireXApp

Core FireX application and task libraries. The `firexkit`, `firex_blaze`,
`firex_bundle_ci`, `firex_keeper`, and `firex_flame` import namespaces are
included in the `firexapp` distribution, along with the `firex_flame_ui`
frontend resource package.

## Releases

The version is stored in `pyproject.toml`; tags trigger publication but do
not determine the version.

1. Bump the version:

```console
uv version --bump patch
```

2. Review, commit, and push `pyproject.toml` and `uv.lock`, then wait for the
   untagged pipeline to pass.
3. Tag that exact green commit and push only the release tag:

```console
git tag "$(uv version --short)"
git push origin "refs/tags/$(uv version --short)"
```
Do not use a `v` prefix or `git push --tags`. Published versions and tags are
immutable; fixes require a new version.
