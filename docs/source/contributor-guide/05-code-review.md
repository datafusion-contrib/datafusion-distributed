# Code review

Review changes for user-visible correctness and maintainability. Formatting and
common lint issues belong to CI; a review should focus on risks that those
checks cannot identify.

## Review workflow

First, classify the intent of the pull request. It can fall into several
categories, and the review workflow changes depending on the category:

- Feature additions (PR prefixed with `feat:`)
- Bug fixes (PR prefixed with `fix:`)
- Refactors (PR prefixed with `refactor:`)
- Docs and examples (PR prefixed with `docs:`)
- Performance (PR prefixed with `perf:`)

## AI-assisted reviews

AI-assisted reviews may prepare feedback for a human reviewer, but must never
submit, approve, or request changes on a pull request. A request to review a
GitHub PR authorizes the agent to create exactly one pending review, unless the
user explicitly asks for a local-only review or says not to post it. The agent
must create that draft before returning its review result. Omit the review event
and review-summary body, then verify that GitHub reports the review as
`PENDING`; the authenticated human submits it later in GitHub.

Every publishable finding must be an inline comment on a line visible in the PR
diff. Each comment needs a non-empty body plus `path`, `line`, and `side`. A
multi-line comment also needs `start_line` and `start_side`; both endpoints and
every intervening line must be visible in the PR diff. Do not publish issue
comments, PR conversation comments, standalone review comments, or a review
summary. Keep findings that cannot be attached to a PR-diff line in the local
review report and state why they were not posted. Even when there are no
findings, create an empty pending review so the human can submit the final
decision.

These rules are instructions, not a hard technical boundary. An environment
that gives an agent unrestricted GitHub credentials must add an external hook
or sandbox restriction if it needs to prevent an agent from bypassing them.

## Feature additions

These PRs add a new capability or address an existing limitation. The proposed
feature needs to be clearly stated, and a PR should not mix multiple features
without good justification.

These PRs are typically large, so it is recommended to split preparatory work
into an isolated PR. Preparatory refactors and other contributions that unlock
the intended feature are better contributed as preliminary PRs.

When reviewing feature additions, reviewers need to weigh the amount of
complexity added to the codebase against the value of the feature. A feature
whose benefit is unclear but introduces substantial complexity should not be
accepted, and the author should be prompted to find a simpler solution.

Some requirements for these types of PRs:

- The PR needs to clearly state the new feature in the PR description.
- A PR should not mix multiple features; split it instead.
- The new feature must be covered by new integration tests and, optionally,
  small, scoped unit tests for finer-grained pieces. Tests need to adhere to the
  testing guidelines in [tests.md](./03-tests.md).
- Any additions to the public API need to be documented properly.
- The new capabilities need to be documented, and an example needs to be added.

## Bug fixes

The change should be accompanied by a test that fails on `main` but succeeds on
the PR.

The bug fix needs to be scoped. When fixing several unrelated things, prefer
opening separate PRs.

The added tests need to adhere to the test guidelines in
[tests.md](./03-tests.md).

## Refactors

These PRs can range from large and mechanical to small and scoped.

The most important thing to verify is that these PRs should either maintain or
remove complexity from the codebase, not introduce it. For example, if a PR is
net-zero in terms of features and fixes but adds a lot of LOC that are not
examples or documentation, that is a red flag for a bad refactor.

For refactors that move large chunks of code around, the git diff might not be
very helpful, so it's important to verify manually that the actual chunks of
code did not change, or if they did, to make sure the changes are harmless.

There is no need for refactors to be covered by new tests. Existing tests are
assumed to catch regressions, as long as the refactor does not silently
introduce new behavior.

These types of PRs are better isolated and scoped, and should not be mixed with
feature additions or other categories of PRs.

## Docs and Examples

Docs should be brief and concise, reducing AI slop as much as possible and going
straight to the point.

Prefer real code samples for explaining concepts, and ASCII drawings for more
complex explanations.

Examples should be brief; ideally, a whole executable example should be under
300 lines of code. Runnable examples should always be accompanied by a
co-located `.md` file (see existing examples).

## Performance

The PR should clearly show improvements in existing benchmarks, taking into
account the guidelines from [benchmarks.md](./04-benchmarks.md).

Complexity introduced by a PR needs to be clearly justified. A PR that
introduces a large amount of code just for the sake of a small performance
improvement should be questioned. Always weigh complexity against real
performance improvements; typically, a PR that improves performance should not
be very large (under 300 LOC of added code).

When reviewing these PRs, the first step is to reproduce the claimed performance
improvements yourself. Only after this is verified can the actual code review
begin.

The PR description should show clear steps for reproducing the performance
improvement.

If there are no current benchmarks that exercise the claimed performance
improvement, a preliminary PR adding those benchmarks may be appropriate.
