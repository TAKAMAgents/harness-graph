# Source-safe Codex fixture

This fixture preserves record families and field shapes observed in real Codex
exporter sessions from 2026-02-16 through 2026-07-18. Message text, command
arguments, instruction bodies, paths, identifiers, and provider content were
replaced with deterministic non-sensitive values before inclusion.

It is a contract fixture for the real exporter format, not a simulated provider
or fake repository. `raw/rollout.jsonl` remains the canonical source and the
metadata/checksum files follow the exporter bundle contract.
