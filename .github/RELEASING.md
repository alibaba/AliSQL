# Release Builds

The release workflow builds native x86_64 and aarch64 packages in Oracle Linux 7,
installs them, creates glibc 2.17 tarballs, and starts a server from each relocated
archive. Publishing runs then upload both packages to GitHub Releases.

## Validate Before Publishing

Run the workflow on the branch to validate. The tag input names the candidate
version; it does not create a tag when `publish=false`:

```bash
gh workflow run build-and-release.yml \
  --repo p1p1bear/AliSQL \
  --ref release-binlog-realtime-duckdb-20261016-squashed \
  -f tag=AliSQL-8.0.44-3 -f publish=false
```

Manual runs default to `publish=false`. They build the selected branch's commit
and upload Actions artifacts only:

- `alisql-x86_64` and `alisql-aarch64`: packaged binaries.
- `smoke-x86_64` and `smoke-aarch64`: installation/runtime check logs.
- `release-notes`: the generated release body for review.

The smoke check verifies the injected version, InnoDB and DuckDB reads/writes,
BRR defaults when present, and the new DuckDB controls when present. It also
checks clean shutdown. This is package validation, not a replacement for MTR.
Validation does not create a release, push a tag, or trigger Docker publishing.

## Prepare Release Notes

The renderer selects the latest dated English changelog for the MySQL base
version from the source being built. It requires the corresponding Chinese
file and rewrites relative documentation links to that repository/ref.
Older-tag retries use their own changelog rather than the current branch's.

For the next candidate after `AliSQL-8.0.44-2`:

```bash
python3 .github/scripts/release-notes.py \
  --source-root . --repository alibaba/AliSQL --tag AliSQL-8.0.44-3 \
  --output /tmp/AliSQL-8.0.44-3-release-notes.md
```

The source for this release is the
[2026-10-16 English changelog](../wiki/changes/changes-in-alisql-8.0.44.2026-10-16-en.md)
and its [Chinese translation](../wiki/changes/changes-in-alisql-8.0.44.2026-10-16-zh.md).
The generated download/tag links become available after the official tag and
assets are published.

## Publish After Merge

After the reviewed changes are merged, choose the release commit and tag.
Either of these events starts a publishing run:

1. Push a tag matching `AliSQL-8.0.*` or `AliSQL-5.7.*`.
2. Publish a GitHub Release for a tag in one of those series.

Do not initiate both paths for the same release; two user-generated events can
queue duplicate builds. A release created by the workflow's `GITHUB_TOKEN`
does not recursively trigger another release workflow.

To retry an existing tag with the workflow from master:

```bash
gh workflow run build-and-release.yml --repo alibaba/AliSQL --ref master \
  -f tag=AliSQL-8.0.44-3 -f publish=true
```

Publishing builds the existing tag, not the selected workflow branch. Both
architecture jobs and the notes job must pass before release assets are created
or replaced. Uploads retain the existing package naming convention:

- `alisql-8.0.44-3-linux-glibc2.17-x86_64.tar.xz`
- `alisql-8.0.44-3-linux-glibc2.17-aarch64.tar.xz`

## Docker Follow-up

After an official `alibaba/AliSQL` publishing run uploads the binary assets,
`trigger-docker` uses `DOCKER_REPO_PAT` to push the same tag to
`p1p1bear/alisql-docker`. That repository downloads the assets and builds the
`songhuaxiong/alisql` amd64/arm64 image using its Docker Hub credentials.

Fork validation skips this job. A successful AliSQL workflow confirms that a
Docker build was triggered, not that the external Docker workflow completed.
Check the [Docker workflow](https://github.com/p1p1bear/alisql-docker/actions/workflows/build.yml)
before announcing image availability. If the Docker tag already exists, retry
the Docker repository workflow for that version rather than moving the tag.
