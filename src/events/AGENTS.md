# Event handlers guide

This guide applies only to `src/events/`. It supplements the
repository-root `AGENTS.md`; do not repeat repository-wide conventions here.

## Scope

This module contains event handler specifications where users can wire up their
own behavior for reacting to certain events during the lifetime of a 
distributed query.

## Guidelines

All event handlers here should follow the same consistent structure, and they
should be made in a way where extending them does not require breaking changes.

All handlers contributed here must use the common tools shipped in 
[common.rs](./common.rs) scoped to this module.

This project ships some sane defaults under [defaults/](./defaults), when 
contributing changes, prefer modeling them in terms of existing event 
handler implementations, rather than inlining behavior in other parts of the 
codebase.