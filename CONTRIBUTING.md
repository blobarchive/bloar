# Contributing

Issues and focused pull requests are welcome. For security-sensitive reports,
use [SECURITY.md](SECURITY.md) rather than a public issue.

Before changing code, read the relevant invariant in `docs/spec.md` and the
operational consequences in `docs/operations.md`. BlobArchive treats
publication, replay, coverage, retention, and crash ordering as correctness
boundaries; a locally successful fetch is not enough to change one safely.

## Development checks

The root `go.mod` toolchain directive is the single Go version source.

```sh
make build
make test
make lint
make conformance
```

`make conformance` is a separate, heavier module because it imports Nitro's
dependency graph. Run focused package tests while iterating, then run the
complete relevant checks before opening a pull request.

Changes to deployment shell, public configuration schemas, or the standalone
Kubo integration should also run their documented focused checks.

## Pull requests

Keep changes narrowly scoped. State:

- the invariant or operator outcome being changed;
- the failure mode before the change;
- the evidence which distinguishes success from a vacuous pass;
- compatibility or migration effects; and
- rollback behavior for deployment changes.

Add regression tests at the narrowest seam which can prove the property.
Measurements from one host or network must be labeled as bounded observations,
not generalized into protocol guarantees.

By contributing, you agree that your contribution is licensed under the
Apache License, Version 2.0.
