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
   TAG=v0.3.0  # or v0.2.0, v1.0.0, etc.
   git tag $TAG
   git push origin $TAG
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

## Releasing the TypeScript package

[`typescript/`](typescript/) is published to npm as `medallion-pipeline`. It has its own version in [`typescript/package.json`](typescript/package.json) and is tagged `ts-vX.Y.Z`. Both paths below run `npm publish`, which first rebuilds and runs the end-to-end test; a failure blocks the release. npm rejects re-uploads, so fix a broken release with a new version.

Bump the version, commit and push to `main`:

```bash
cd typescript
npm version patch --no-git-tag-version   # or minor, major
```

Then use one of the two paths.

### Through GitHub Actions

Needs the trusted publisher (see below). [`release-typescript.yml`](.github/workflows/release-typescript.yml) fails if the tag doesn't match `package.json`, and skips publishing a version already on npm.

```bash
TAG=ts-v0.1.1
git tag $TAG
git push origin $TAG
```

### By hand

From a clean checkout of the pushed commit:

```bash
cd typescript
npm ci
npm login
npm publish
```

Then tag and push as above. The workflow sees the version on npm and skips it.

### First publish and trusted publisher (one-time)

The package must exist on npm before a trusted publisher can be set up, so publish `0.1.0` by hand, without a version bump. Then, on npmjs.com under the package's Settings → Trusted publishing → GitHub Actions:

- Organization or user: `Mcklmo`
- Repository: `medallion`
- Workflow filename: `release-typescript.yml`
- Environment name: leave empty
- Allowed actions: allow `npm publish` (by default only `npm stage publish` is allowed)

A new trusted publisher expires unless it publishes successfully within 2 days, so set it up right before the first release through GitHub Actions. After that release works, set Settings → Publishing access to "Require two-factor authentication and disallow tokens".

## Setup for Google Cloud project

- [ ] Service account roles: Pubsub Admin, Artifact Registry Admin, Cloud Run Admin, Storage Object Admin, Cloud Scheduler Admin, Secret Manager Secret Accessor
- [ ] Enable APIS: PubSub API, Artifact Registry API, Cloud Run Admin API, Cloud Scheduler API, Secret Manager API
- [ ] Create an Artifact Registry repository and a Storage Bucket with the same name as the "repo" in the config.yml file.
- [ ] Create one API key in GCP Secrets per extractor, named "EXTRACTOR_NAME-api-key". EXTRACTOR_NAME is enforced to start with "extract-".

At some point, you get this error or similiar `403 Permission 'iam.serviceaccounts.actAs' denied on service account 628431593486-compute@developer.gserviceaccount.com (or it may not exist).` Copy the email of the compute account and replace it in the below command. Also paste your service account email that performs this deployment.

```bash
gcloud iam service-accounts add-iam-policy-binding \
  628431593486-compute@developer.gserviceaccount.com \
  --member="serviceAccount:YOUR_DEPLOYER_IDENTITY" \
  --role="roles/iam.serviceAccountUser" \
  --project="medallion-test"
```

## Docker compose debugging

To run a debugger for any compose service, update the `docker-compose.debug.yml` file with the correct service name and run:

```bash
docker compose -f docker-compose.yml -f docker-compose.debug.yml up
```

or

```bash
docker compose -f docker-compose.yml -f docker-compose.debug.yml up --build
```

Once all containers are ready, run the debugging configuration `Attach to docker service`

## TypeScript package

In `typescript/`, with Node 22.18 or later:

```sh
npm ci
npm test
```
