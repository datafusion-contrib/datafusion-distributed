# DataFusion Distributed agent guide

## Project purpose

DataFusion Distributed is a Rust library that adds distributed execution to
Apache DataFusion. It is deliberately a library, not a complete distributed
engine: preserve its familiar DataFusion API and avoid imposing assumptions
about a user's networking or deployment stack.

## Repository map

- `src/`: library implementation.
    - `codec/`: Protobuf codecs for the execution plans this project ships +
      machinery for wiring up custom user-defined codecs.
    - `common/`: internal shared helpers. These are collections of reusable
      functions that are not coupled to anything else in the project, they are
      standalone helpers.
    - `coordinator/`: Main execution plan implementation in charge of sending
      plans to workers and execute them.
    - `distributed_planner/`: transforms DataFusion physical plans for
      distributed execution. See further instructions in
      [src/distributed_planner/AGENTS.md](./src/distributed_planner/AGENTS.md)
    - `events/`: event handlers where users can customize how this project
      reacts to certain distributed events. See further instructions in
      [src/events/AGENTS.md](./src/events/AGENTS.md)
    - `execution_plans/`: distributed physical-plan nodes and test-only
      execution benchmarks. Any ExecutionPlan implementation should be
      contributed here as its own module.
    - `metrics/`: task-metric collection and distributed-plan metric rendering.
    - `protocol/`: coordinator-worker messages, channel resolution, and gRPC
      transport. Anything gRPC specific should be contributed to
      `protocol/grpc`.
    - `test_utils/`: shared test fixtures, plans, resolvers, and assertions.
    - `work_unit_feed/`: work-unit definitions, providers, registries, and
      remote-feed support.
    - `worker/`: worker service, sessions, task execution, and connections.
- `tests/`: integration and correctness tests.
- `examples/`: runnable usage examples.
- `benchmarks/`: local benchmark crate and microbenchmarks; do not change
  benchmark behavior as an incidental part of a library change.
- `docs/`: Sphinx documentation. Contributor documentation is in
  `docs/source/contributor-guide/`.

## Always

- Keep changes narrowly scoped; do not include unrelated refactors or dependency
  upgrades.
- Do not introduce a public or wire-format breaking change unless it is
  explicitly requested.
- For every public breaking change, add a user-facing migration entry to the
  next major upgrade guide under `docs/upgrade/`. Derive its version from the
  latest release tag: after a `2.x.y` release, update `docs/upgrade/3.0.0.md`.
  Describe only the code or configuration adaptation users need to make.
- Keep public documentation and runnable examples aligned with public API
  changes.
- Follow established patterns in the closest relevant code and tests before
  introducing a new abstraction or mechanism.
- Reuse existing helpers, extension points, and test fixtures when they fit.
- Introduce a new pattern only when existing ones cannot express the required
  behavior; the rationale for this should be clearly and briefly stated.
- When claiming a performance improvement, never move a PR out of draft before
  first qualifying the performance benefits following the guidelines in
  `docs/source/contributor-guide/04-benchmarks.md`.
- When submitting PRs, make sure they are scoped to one thing, and that they
  fall in one of these categories:
    - Feature additions (PR prefixed with `feat:`)
    - Bug fixes (PR prefixed with `fix:`)
    - Refactors (PR prefixed with `refactor:`)
    - Docs and examples (PR prefixed with `docs:`)
    - Performance (PR prefixed with `perf:`)

## Task-specific guidance

Read the relevant contributor guide before starting work in these areas:

- Code review: `docs/source/contributor-guide/05-code-review.md`
- Tests: `docs/source/contributor-guide/03-tests.md`
- Performance and benchmark changes:
  `docs/source/contributor-guide/04-benchmarks.md`

## Validation

Run the narrowest relevant check first. Use the repository's standard format,
lint, test, and documentation commands as appropriate; the contributor guide
documents the full validation matrix.

## Code review

Read the code-review guide and the closest scoped `AGENTS.md`. Report concrete
correctness risks, not style issues covered by CI.

### Publishing AI review drafts

When explicitly authorized to publish GitHub PR review feedback, create a
*pending* review only. Never submit, approve, request changes, or publish a
review autonomously. Create the review with its event omitted and with no
review-summary body, then confirm that GitHub reports its state as `PENDING`.

Every finding must be an inline comment attached to one changed line or one
contiguous range of changed lines. Do not use issue comments, PR conversation
comments, standalone review-comment endpoints, or a review summary. Findings
that cannot be attached to a changed line must remain in the local review
report. Each inline comment must include a non-empty body, `path`, `line`, and
`side`; ranges must also include both `start_line` and `start_side`.
