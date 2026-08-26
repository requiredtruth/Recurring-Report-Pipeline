# Project specification

Status: DONE for 0.1.0 after the acceptance commands in `DONE.md` pass.

## Purpose

Turn recurring local CSV inputs into validated, normalized, transformed, delivered reports with durable factual run evidence and visible failure state.

## Invariants

- No output or delivery is published before input, transformation, and quality checks pass.
- Input, output, and delivery paths remain inside explicit roots.
- A pipeline/scheduled-time pair runs at most once.
- All durable state carries a schema version and SQLite writes are transactional.
- Logs cover material actions only and persist neither source rows nor config bodies.
- The bundled demo is synthetic, deterministic, and self-contained.
- `run.sh` launches the GUI; `cli.sh` preserves automation behavior; `install.sh` installs and verifies both.

## Acceptance

- Demo produces hashed CSV, JSON, HTML, manifest, and local delivery artifacts.
- Missing columns, blank output, invalid types, bounds, duplicate schedule slots, and stale runs fail deliberately.
- GUI constructs offscreen and exposes actual register/run/demo/status actions.
- Unit/integration tests, shell syntax checks, editable package build, clean install, privacy scan, and CI pass.
