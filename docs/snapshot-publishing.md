# Publishing CivicSodaQuack snapshots

This guide shows how to use the reusable `csq snapshot` workflow to automate
nightly snapshots of one or more Socrata portals to a public destination.

## Caller workflow example

In your own repo (the one that owns the portal YAML configs and credentials),
create `.github/workflows/nightly.yml`:

```yaml
name: Nightly snapshot
on:
  schedule:
    - cron: "0 3 * * *"
  workflow_dispatch:

jobs:
  chicago:
    uses: neomantra/CivicSodaQuack/.github/workflows/snapshot.yml@main
    with:
      portal-yaml-path: portals/chicago.yaml
      release-target: s3
      s3-bucket: my-snapshot-bucket
      index-url-base: https://my-snapshot-bucket.s3.amazonaws.com
    secrets:
      socrata-app-token: ${{ secrets.SOCRATA_APP_TOKEN }}
      aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
      aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

`portals/chicago.yaml` is your existing CivicSodaQuack portal config (the same
file `csq sync` consumes locally).

## Inputs and secrets

See `.github/workflows/snapshot.yml` in this repo for the full list. Key fields:

- `portal-yaml-path` — path inside the caller's checkout to the portal config.
- `release-target` — `github-release` (publishes via `gh release create`) or
  `s3` (uploads via `aws s3 cp`).
- `index-url-base` — public URL prefix; combined with the tarball filename
  becomes the entry URL agents fetch.
- `index-path` — where the workflow writes the index file. For S3 publishing,
  this is uploaded alongside the tarball so consumers can fetch
  `<index-url-base>/index.json`.

## Consuming the snapshot

Once published, agents on other hosts fetch the latest via the index:

```bash
csq fetch --index https://my-snapshot-bucket.s3.amazonaws.com/index.json
```

Pin to a specific snapshot id:

```bash
csq fetch \
  --index https://my-snapshot-bucket.s3.amazonaws.com/index.json \
  --snapshot 01HZ...
```

`csq fetch` verifies the SHA-256 against the manifest inside the tarball
before declaring success.
