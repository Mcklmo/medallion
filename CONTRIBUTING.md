# Contributing

## Developer notes

User-facing example pipelines live in [`example/__init__.py`](example/__init__.py). To execute them via the `Run User Pipeline (example)` launch configuration (or directly from the CLI), the `MEDALLION_ROOT` environment variable must be set to that folder so the resolver can locate the user package.

## Releasing

Releases are automated: pushing a tag matching `v*` to `main` triggers [`.github/workflows/release.yml`](.github/workflows/release.yml), which builds the package with Poetry and publishes it to PyPI via OIDC trusted publishing (no API token needed).

### Cutting a release

1. Bump `version` in [`pyproject.toml`](pyproject.toml) (semver: patch for bug fixes, minor for new features, major for breaking changes).
2. Commit and push to `main`.
3. Tag and push the tag — the tag's version *must* match `pyproject.toml`, or the workflow fails its sanity check:

   ```bash
   git tag v0.1.3
   git push origin v0.1.3
   ```

4. PyPI rejects re-uploads of an existing version. If a release ships broken, bump again — don't try to overwrite.

### Local build (optional, but recommended for some changes)

```bash
poetry build
ls dist/
unzip -l dist/medallion_pipeline-*-py3-none-any.whl   # inspect contents
```

Worth doing before tagging when:

- You edited `pyproject.toml` (entry points, dependencies, classifiers, license).
- You added non-Python files (`py.typed`, data files, templates) — Poetry can silently omit these.
- You're unsure the package still imports cleanly.

For pure code changes, skip the local build and just push + tag.

### Pre-release / dry run

Push an `rc` tag first to publish to PyPI as a pre-release (won't be installed by default by `pip install medallion-pipeline`):

```bash
git tag v0.1.3rc1
git push origin v0.1.3rc1
```

Verify on <https://pypi.org/project/medallion-pipeline/>, then cut the real tag.

### Initial PyPI trusted-publisher setup (one-time, already done)

For reference, if the trusted publisher ever needs to be reconfigured: <https://pypi.org/manage/account/publishing/> → pending publisher with owner `Mcklmo`, repo `medallion`, workflow `release.yml`, environment `pypi`.
