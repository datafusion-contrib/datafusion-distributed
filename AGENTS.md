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
- Use DataFusion's error macros instead of manually constructing error variants or
  formatting error strings. Use `exec_err!`, `internal_err!`, etc. when returning
  a `Result`, and `exec_datafusion_err!`, `internal_datafusion_err!`, etc. in
  `map_err` or `ok_or_else`. Import the macros at the top of the file and use `?`
  for errors with existing conversions. This applies to benchmark and tooling code too.
- Introduce a new pattern only when existing ones cannot express the required
  behavior; the rationale for this should be clearly and briefly stated.
- When claiming a performance improvement, never move a PR out of draft before
  first qualifying the performance benefits following the guidelines in
  `docs/source/contributor-guide/04-benchmarks.md`.
- Prefer adding `use` statements at the top of the file. Only use fully
  qualified import paths inside a code block for disambiguating between 
  functions also present in the standard library (e.g., `tokio::time::sleep`).
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
- Running, comparing or profiling benchmarks: `.agents/skills/benchmark/SKILL.md`

## Test design and review

Tests are executable documentation. A reviewer should understand the scenario,
configuration, operation, and expected behavior by reading the test itself.
Optimize for that understanding, not for the fewest lines.

### Make each scenario readable in place

- Give each behavioral scenario a named, directly runnable test. Do not use
  generated test macros or loops to hide distinct integration-test cases.
  Loops are fine for fixture construction or checking one coherent invariant.
- Keep scenario-defining configuration visible in one harness builder chain:
  execution mode, workers, partitions, enabled features, and relevant limits.
  Configure it before building the harness.
- Do not introduce helpers such as `harness(true, false)` or
  `assert_case(config, expected)` that hide the scenario, execution, and
  assertions together. Modest repetition between named tests is preferable.
- Prefer inline `insta` snapshots for readable plans, results, assignments,
  metrics, and errors. Keep compact expectations beside the operation being
  tested. Use external snapshots only when their size makes inline expectations
  materially harder to read.
- Use ordinary assertions for simple typed values. Do not manually reproduce
  snapshot testing with many string comparisons or substring checks.

### Share infrastructure without hiding intent

- Reuse the existing harness and fixture builders. Put session construction,
  codec wiring, worker startup, and reusable fixture mechanics in test tooling.
  Tests select the configuration explicitly through that tooling.
- Prefer existing builder methods and configuration hooks. Do not add a new
  harness method merely to abbreviate an existing setting. Add an abstraction
  when it encapsulates reusable setup or a meaningful domain operation.
- Keep fixture helpers focused on constructing data or metadata. They must not
  silently choose execution mode, worker count, planner settings, or assertions.
  Make relevant fixture contents and properties apparent at the call site.
- Never add constructor or registration variants named `with_x_and_y`,
  `with_x_with_y`, or equivalent combinations of options. Put independent
  settings in the existing options struct or composable builder methods, and
  pass them through the harness's existing configuration hooks.
- Use the harness for session construction, codec registration, and worker
  setup instead of recreating that plumbing in tests. Keep scenario-defining
  options visible in each test's builder chain; helpers must not silently
  install a different execution mode or configuration.
- Reuse existing fixtures where they fit; use the fixture builders when the
  scenario requires a different shape. Avoid extensive ad hoc patching of a
  fixed dataset.
- Put helper implementations below the tests. Moving boilerplate into a helper
  is useful only if the test remains understandable without opening that helper.

### Exercise the real contract

- Prefer integration tests through public APIs and the normal planning and
  execution path. Do not expose private fields or add production APIs solely
  to make tests inspect implementation details.
- Integration tests should resemble application usage. Do not insert manual
  codec roundtrips, private-state reconstruction, or synthetic routing behavior
  into unrelated tests. Exercise transport through the harness; test codec
  encoding, decoding, and invalid messages directly when those are the subject.
- Assert the contract the feature promises. Equal query results do not imply
  equal plans, statistics, assignments, or internal representations.
- Make assertions demonstrate the intended behavior: show selected values as
  well as counts when selection matters; show task loads when balancing matters;
  inspect runtime placement when runtime separation is the feature.
- Inspect assignments through supported plan/source APIs or observable metrics.
  Do not treat private feed emission order as a public contract. When assignment
  policy is the subject, present it as a readable behavioral snapshot.

### Keep coverage focused

- Use one integration-test file per feature. Keep small unit tests next to their
  implementation for isolated logic or edge cases that are impractical to
  express through realistic integration scenarios.
- Every new test should identify a distinct failure it catches. Avoid overlapping
  coverage across unit, integration, and SQL tests without a concrete reason.
- Do not add tests solely to prove that a test helper or fixture works when
  feature tests already exercise it. Do not add redundant tests around benchmark
  scaffolding that is exercised by running the benchmark.
- For a regression test, verify that it detects the original failure. Passing
  after the change is not sufficient evidence that it covers the bug.

Before submitting, read each changed test without following helper definitions
or opening snapshot files. If the setup or expected behavior is unclear, revise
that test before considering it ready for review.

## Validation

Run the narrowest relevant check first. Use the repository's standard format,
lint, test, and documentation commands as appropriate; the contributor guide
documents the full validation matrix.

## Code review

Read the code-review guide and the closest scoped `AGENTS.md`. Report concrete
correctness risks, not style issues covered by CI.

### Publishing AI review drafts

When asked to review a GitHub PR, first render every proposed review comment in
the response, including its file and line or line range. Then ask the user if
they want to create a pending review containing those exact comments. Do not
make a GitHub write until the user answers affirmatively. If the user asks to
revise the comments, render the revised set and ask again.

After affirmative confirmation, create exactly one *pending* review. Never
submit, approve, or request changes autonomously. Create the review with its
event omitted and with no review-summary body, then confirm that GitHub reports
its state as `PENDING`. If there are no findings, render that result and ask
whether the user wants an empty pending review.

Every publishable finding must be an inline comment attached to one PR-diff
line or one contiguous range of PR-diff lines. Do not use issue comments, PR
conversation comments, standalone review-comment endpoints, or a review
summary. Each inline comment must include a non-empty body, `path`, `line`, and
`side`; ranges must also include both `start_line` and `start_side`. If a
finding cannot be attached to a PR-diff line, report it locally and say why it
cannot be posted. After confirmation, publish only the rendered, attachable
comments.
