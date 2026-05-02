"""Dispatcher for ``python -m aeroza.cli <subcommand>``.

Subcommands map 1:1 to modules in :mod:`aeroza.cli`. Adding a new one is
a two-step change: drop a new module that exposes ``main(argv) -> int``,
then register its name here.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from aeroza.cli import ingest_alerts, ingest_mrms, materialise_mrms

SUBCOMMANDS: dict[str, Callable[[list[str] | None], int]] = {
    "ingest-alerts": ingest_alerts.main,
    "ingest-mrms": ingest_mrms.main,
    "materialise-mrms": materialise_mrms.main,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="aeroza", description="Aeroza command-line entry-points.")
    parser.add_argument(
        "subcommand",
        choices=sorted(SUBCOMMANDS.keys()),
        help="Subcommand to run.",
    )
    parser.add_argument(
        "rest",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the chosen subcommand.",
    )
    args = parser.parse_args(argv)
    return SUBCOMMANDS[args.subcommand](args.rest)


if __name__ == "__main__":
    sys.exit(main())
