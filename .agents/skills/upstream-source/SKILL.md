---
name: upstream-source
description: Build this workspace against local checkouts of apache/datafusion and apache/arrow-rs (arrow-*, parquet) at exactly the revision published on crates.io. Use when profiling, debugging or a regression points into an upstream crate, or when preparing a fix to send upstream.
---

# Linking upstream DataFusion and arrow-rs source

- Clones live next to the main datafusion-distributed checkout: `$UPSTREAM/datafusion` and
  `$UPSTREAM/arrow-rs`, where
  `UPSTREAM=$(cd "$(git rev-parse --path-format=absolute --git-common-dir)/../.." && pwd)`.
  The user may have work in progress in them, so link a detached worktree named after the
  version instead (`$UPSTREAM/arrow-rs-<version>`), e.g.
  `git -C "$UPSTREAM/arrow-rs" worktree add --detach "$UPSTREAM/arrow-rs-<version>" '<version>^{commit}'`.
- The versions in use are in `Cargo.lock`. These crates are published from the voted release
  tarball, so they ship no `.cargo_vcs_info.json`. The release tag (`<version>`, no `v` prefix)
  is the last RC's commit, which is the published source. Check a crate with
  `diff -r ~/.cargo/registry/src/*/<crate>-<version> <worktree crate dir>`.
- Patch releases come from maintenance branches (`branch-55`, `59_maintenance`), so fetch the
  tag explicitly: `git fetch <apache remote> tag <version>`.
- Link with `[patch.crates-io]` entries in an untracked `.cargo/config.toml`, using absolute
  paths. Every crate of the family that appears in `Cargo.lock` must be patched. Patching only
  `arrow-ipc` builds a second `arrow-array` from the worktree, and its types don't unify with the
  crates.io copy. `cargo metadata --no-deps --format-version 1` in the worktree maps crate names
  to their directories.
- A patch applies only to the locked version. When the lock pins another compatible version
  (e.g. linking arrow 59.3.0 into a DataFusion checkout locked to 59.2.0), cargo silently keeps
  crates.io until you run `cargo update -p <crate>` for each patched crate. Always confirm with
  `cargo tree -i <crate> --depth 0` before measuring.
- Linking rewrites `Cargo.lock`. Never commit it or `.cargo/` (some IDEs stage new files
  automatically). Unlink with `rm -rf .cargo && git checkout Cargo.lock`. Linking and unlinking
  each trigger a full rebuild.
- Worktree edits apply on the next build. To keep a fix, `git switch -c` in the worktree.
  Upstream PRs target `main`, so port the change there. Upstream's own tests need
  `git submodule update --init`.
- Every upstream fix or performance claim must be reproducible in the upstream repo alone.
  Upstream reviewers won't run datafusion-distributed, so its results only motivate the change.
  Demonstrate it with an upstream test, or with the upstream benchmarks: criterion benches in
  each crate (`cargo bench -p <crate> --bench <name> -- --save-baseline before`, then
  `--baseline before`) and DataFusion's `benchmarks/bench.sh`.
- Existing upstream benches may not exercise the conditions where the problem shows up here,
  such as many concurrent tasks or larger batches. Single-threaded benches miss allocator
  contention, for example. Extend them until the effect reproduces, and report any cases that
  regress.
