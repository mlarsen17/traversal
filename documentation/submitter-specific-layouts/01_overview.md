# Submitter-Specific Layouts with File-Type Canonical Schemas: Overview

## Purpose

Evolve `traversal` so raw ingest layouts are resolved per `(submitter_id, file_type)` while canonical schemas remain owned by `file_type`.

This change keeps the canonical contract stable at the statewide file-type level, but allows intake/parse/validation to correctly interpret heterogeneous submitter formats for the same file type.

## Why the current model is insufficient

Today, layout resolution is effectively keyed by `(file_type, layout_version)` and filename conventions can hardcode default layout versions by file type. That is too coarse once multiple submitters send distinct active layouts for the same file type.

Key gaps in the current model:

- Intake cannot reliably disambiguate concurrent submitter-specific layouts for one file type.
- Layout identity is not explicit enough for submitter-aware routing decisions.
- File naming rules currently mix responsibilities (classification + global layout selection).

At the same time, the repo already separates submitter-gold and canonical-gold concerns, and canonical mappings are already data-driven and attached to `layout_id` downstream. This provides a natural extension path.

## Target architecture

### Core model

- **Layout registry row**: belongs to one concrete `(submitter_id, file_type)` context, with versioning.
- **Canonical schema**: belongs to `file_type` (and schema version), not submitter.
- **Canonical mapping**: belongs to concrete `layout_id` and maps into a canonical schema version for that file type.

### Registry and resolution direction

Move from implicit layout identity `(file_type, layout_version)` to submitter-aware identity, ideally:

- `layout_key` / `layout_code` (stable logical key)
- `layout_version`
- scoped by `submitter_id` + `file_type`

Update intake resolution from:

- `filename -> file_type + layout_version -> layout_id`

to:

- `submitter_id + filename (+ optional header sniff) -> file_type`
- `submitter_id + file_type + filename/header -> layout_id`

### Suggested submitter-aware config layer

Introduce a control-plane concept (for example, `submitter_file_config`) to hold per-submitter routing rules:

- filename patterns
- allowed extensions
- delimiter/header expectations
- default concrete layout selection
- optional header signature columns
- optional effective dating/routing hints

This keeps routing decisions data-driven instead of hardcoded in global filename conventions.

## Fit with current `traversal` P0–P5 architecture

This proposal is additive and aligns with current pipeline boundaries:

- **P0/P1 intake** resolve a concrete submitter-specific `layout_id`.
- **P2 parse** continues to parse by resolved `layout_id`.
- **P3 validation** remains layout-aware.
- **P4 submitter gold** remains keyed per `(state, file_type, submitter_id, atomic_month)`.
- **P5 canonical gold** remains keyed per `(state, file_type, atomic_month)`.

The canonical builder should select mapping based on the winning submission's concrete `layout_id`, preserving lineage while producing a file-type-specific statewide canonical output.

## End-to-end flow after the change

1. Intake receives submission with known `submitter_id`.
2. Submitter-aware config classifies `file_type` (filename + optional header sniff).
3. Intake resolves concrete `layout_id` for `(submitter_id, file_type, payload signals)`.
4. Parse runs using that layout's schema/parser config.
5. Validation executes rule sets with existing layout-aware semantics.
6. Submitter-month winner selection remains unchanged at P4 grain.
7. Canonical build for each `(state, file_type, atomic_month)` reads winners and chooses canonical mapping from each winner's concrete `layout_id`.
8. Canonical output remains a single statewide partition per file type/month.

## Design principles

- Keep canonical contracts stable at file-type scope.
- Make layout resolution explicit, deterministic, and submitter-aware.
- Keep routing/config in control-plane data rather than hardcoded global rules.
- Preserve lineage from canonical rows back to concrete source layout/submission.
- Prefer backward-compatible migration steps to avoid pipeline interruption.

## Non-goals

- No change to canonical partition grain or statewide semantics.
- No new submitter-specific canonical schemas.
- No rewrite of parse/validation business logic beyond layout lookup plumbing.
- No unrelated redesign of winner-selection policy.

## Why canonical remains file-type-specific

Canonical gold is the statewide convergence layer used for downstream consumers. Keeping canonical schema ownership at `file_type` preserves:

- a single stable interface per dataset type,
- consistent cross-submitter analytics,
- lower downstream coupling,
- compatibility with existing P5 storage layout and control-plane semantics.

Submitter variability should be absorbed at layout + mapping layers, not pushed into canonical schema proliferation.
