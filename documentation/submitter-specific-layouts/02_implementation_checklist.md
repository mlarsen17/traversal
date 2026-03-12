# Submitter-Specific Layouts with File-Type Canonical Schemas: Implementation Checklist

## Scope

Planning artifact for implementing submitter-aware layout identity and intake resolution in the `traversal` repository, while preserving file-type-specific canonical schemas and canonical output grain.

This document is implementation-oriented but intentionally excludes executable code.

---

## MVP goal

Enable multiple submitters to have distinct active layouts for the same `file_type`, with intake resolving a concrete submitter-specific `layout_id` and canonical mapping chosen by winner `layout_id`.

## Long-term goal

Make layout resolution and file identification fully data-driven through a submitter-aware control-plane configuration model with clear versioning/effective-dating behavior.

---

## Phase 1: Make layout registry submitter-aware

Likely touchpoints (high level):

- metadata schema/migrations for `layout_registry` identity model
- layout sync/registration asset(s)
- any lookup helpers that currently key by `file_type + layout_version`

Checklist:

- [ ] Define submitter-aware layout identity model:
  - [ ] add `submitter_id` scope to layout records (or equivalent join model)
  - [ ] add/standardize logical layout identifier (`layout_key` or `layout_code`)
  - [ ] keep explicit `layout_version`
- [ ] Define uniqueness constraints/indexes so concurrent submitter layouts for same `file_type` are legal and unambiguous.
- [ ] Preserve existing lineage (`layout_id`) as immutable FK target for downstream runs/builds.
- [ ] Plan backward compatibility for existing layout rows (default submitter behavior, migration mapping, or staged rollout).

MVP path:

- Extend existing table minimally and keep most existing columns/semantics.

Cleaner long-term path:

- Introduce explicit logical layout entity + versioned concrete revisions if needed for stronger governance.

---

## Phase 2: Update layout sync/registration pipeline

Likely touchpoints (high level):

- layout definition files and loader conventions
- `sync_layout_registry` (or equivalent) registration/upsert logic

Checklist:

- [ ] Update layout definition conventions to include submitter scope and logical key.
- [ ] Update sync logic to upsert by submitter-aware identity, not global file-type version identity.
- [ ] Ensure idempotent sync across environments.
- [ ] Emit clear diagnostics when definitions conflict (duplicate identity, version mismatch, incompatible parser metadata).

MVP path:

- Allow both old and new definition formats during transition, with clear deprecation warnings.

Cleaner long-term path:

- Fully standardize on submitter-aware definitions and remove global fallback behavior.

---

## Phase 3: Intake classification and layout resolution changes

Likely touchpoints (high level):

- intake submission classification logic
- filename conventions/helpers
- optional header sniffing utilities
- submission metadata persistence (`layout_id` assignment)

Checklist:

- [ ] Refactor classification pipeline into two explicit decisions:
  - [ ] resolve `file_type` using submitter-aware signals
  - [ ] resolve concrete `layout_id` using `(submitter_id, file_type, filename/header cues)`
- [ ] Stop globally hardcoding layout versions by file type in filename conventions.
- [ ] Add deterministic fallback behavior when multiple candidate layouts match.
- [ ] Add reviewable failure mode (`NEEDS_REVIEW`) when no safe layout match exists.
- [ ] Log routing evidence for operational debugging (which rule/signature selected the layout).

Recommended config concept:

- [ ] Introduce `submitter_file_config` (table/config artifact) with:
  - [ ] filename patterns
  - [ ] extensions
  - [ ] delimiter/header expectations
  - [ ] default layout pointer
  - [ ] optional header signature columns
  - [ ] optional effective dating / routing hints

MVP path:

- Minimal submitter-aware lookup with filename + default layout config.

Cleaner long-term path:

- Full rule-priority engine with header signatures, effective dating, and explicit conflict resolution policy.

---

## Phase 4: Parse/validation compatibility verification

Likely touchpoints (high level):

- parse run initialization where schema/parser config is loaded from `layout_id`
- validation orchestration assumptions around layout provenance

Checklist:

- [ ] Confirm parse remains driven solely by resolved `layout_id` with no hidden global layout-version assumptions.
- [ ] Confirm validation remains layout-aware without additional model changes.
- [ ] Verify operational metadata/reporting fields remain correct when same `file_type` has multiple concrete layouts.

---

## Phase 5: Canonical mapping registration and lookup updates

Likely touchpoints (high level):

- canonical registry sync
- canonical mapping identity/constraints
- canonical month build mapping selection logic

Checklist:

- [ ] Ensure canonical mappings are registered against concrete `layout_id` rows (submitter-aware).
- [ ] Validate mapping lookup in canonical build uses winning submission's `layout_id`.
- [ ] Keep canonical schema ownership/versioning keyed by `file_type`.
- [ ] Add guardrails for missing mapping cases (clear failure reason and remediation path).

MVP path:

- Support one active mapping per concrete `layout_id` + canonical schema target.

Cleaner long-term path:

- Add mapping lifecycle controls (effective dating, supersession metadata, compatibility checks).

---

## Phase 6: Test coverage for multi-submitter same-file-type scenarios

Likely touchpoints (high level):

- integration tests spanning intake -> parse -> validation -> submitter gold -> canonical gold
- fixture data for multiple submitters with same `file_type`, distinct layouts

Checklist:

- [ ] Add test where two submitters share `file_type` but require different concrete layouts.
- [ ] Verify intake picks different `layout_id`s correctly.
- [ ] Verify parse/validation succeed for both submitters.
- [ ] Verify winner selection remains per submitter-month grain.
- [ ] Verify canonical build uses winner `layout_id` mapping and produces unified file-type canonical output.
- [ ] Add negative tests for ambiguous/no-match layout routing.

---

## Phase 7: Rollout, migration, and backfill strategy

Checklist:

- [ ] Sequence deployment so schema + registry changes land before routing behavior switch.
- [ ] Run dual-read/dual-evaluation period if needed (old vs new resolver metrics) before full cutover.
- [ ] Prepare migration/backfill for existing layout metadata to submitter-aware model.
- [ ] Define handling for historical submissions:
  - [ ] no-op (grandfather existing `layout_id`s), or
  - [ ] targeted metadata backfill for consistency/reporting.
- [ ] Publish operational runbook updates for onboarding a new submitter layout.

---

## Assumptions

- `submitter_id` is reliably known at intake time.
- Existing parse/validation steps are already `layout_id`-driven and can remain mostly unchanged.
- Canonical schemas should continue to represent file-type contracts, not submitter contracts.
- Existing P4/P5 grains remain authoritative:
  - submitter gold: `(state, file_type, submitter_id, atomic_month)`
  - canonical gold: `(state, file_type, atomic_month)`

## Risks

- Ambiguous routing when filename patterns overlap across submitter layouts.
- Incomplete mapping coverage for legacy layout rows during transition.
- Operational complexity if config governance is weak (drift between registered layouts and routing rules).
- Temporary mismatch between old filename conventions and new submitter-aware resolver during rollout.

## Open questions

- Should submitter-aware layout identity be encoded directly in `layout_registry` or split into parent/child tables?
- What should be the canonical conflict policy when multiple routing rules match equally?
- Do we need effective dating at MVP, or can we defer to explicit version bumps/manual cutovers?
- How should unknown/novel headers be surfaced for rapid onboarding (alerts, review queue metadata, auto-profiling)?
- What minimum observability fields are required to debug routing decisions in production?
