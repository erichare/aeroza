"""Admin-only / dev-console operations.

Routes here are gated by the ``AEROZA_DEV_ADMIN_ENABLED`` env flag and
intentionally not part of the v1 surface — they expose operations that
are normally driven by the long-running workers (e.g. ingest at a
historical timestamp). The dev console at ``/demo`` calls them so a
visitor can pull the radar grids for a curated event with one click,
instead of pasting a CLI command.
"""
