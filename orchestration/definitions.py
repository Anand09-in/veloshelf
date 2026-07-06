"""Dagster entrypoint (Phase 0 placeholder).

Real assets (features → model → eval → retrain) land in Phase 3-5.
For now this defines a single trivial asset so `dagster dev` boots cleanly
and the UI is reachable at http://localhost:3000.
"""

from __future__ import annotations

from dagster import Definitions, asset


@asset
def veloshelf_healthcheck() -> str:
    """Placeholder asset proving Dagster wiring works."""
    return "veloshelf orchestration online"


defs = Definitions(assets=[veloshelf_healthcheck])