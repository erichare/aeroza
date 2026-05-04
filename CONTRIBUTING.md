# Contributing to Aeroza

Aeroza is a single-codebase platform: FastAPI backend (`aeroza/`), Next.js
web app (`web/`), and a TypeScript SDK (`sdk-ts/`). This guide describes the
workflow for getting changes into `main`.

## Getting started

You need Docker, Node 20+, and [uv](https://docs.astral.sh/uv/).

```bash
make start
```

That single command boots the full local stack — Postgres / Redis / NATS via
Docker, Alembic migrations, FastAPI on `:8000`, the web app on `:3000`, plus
the ingest workers and the webhook dispatcher. See the README's Quickstart
section for what it does step-by-step.

`make stop` tears down Docker. Re-running `make start` is idempotent.

## Branch and commit conventions

- Branch from `main`. Use a short, kebab-case name prefixed with the change
  type: `feat/...`, `fix/...`, `chore/...`, `refactor/...`, `docs/...`.
- Keep commits focused. Squash trivial fixups before pushing.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/):

  ```
  <type>(<scope>): <subject>

  <body explaining the *why*, not the *what*>
  ```

  Examples from `git log`:

  ```
  fix(deploy): run Railway container as root for volume writes
  feat(replay): historical NWS warnings overlay (IEM archive)
  feat(web/map): swap CARTO raster basemap for OpenFreeMap vector
  ```

## Verification before opening a PR

A change is "verified green" when **all** of these pass:

```bash
# Python
uv run ruff check .
uv run ruff format --check .
uv run mypy aeroza
uv run pytest -m "not integration"          # unit suite
uv run pytest -m "integration or grib"      # needs Postgres + eccodes; CI runs this

# Web + SDK
cd web && npx tsc --noEmit && npm run build
cd sdk-ts && npx tsc --noEmit && npm test
```

CI runs the same checks. The unit suite is gated at a coverage floor —
your PR shouldn't drop coverage below that floor.

## Pull request flow

1. **Open a PR** against `main`. The PR description should explain the
   *why* and link any relevant ROADMAP / issue.
2. **CI must be green.** No exceptions for "I'll fix it after merge."
3. **Self-review.** Read your own diff before asking anyone else to.
4. **Merge.** Once verified green and reviewed, squash-merge into `main`.
   Branch-and-commit-then-push-then-PR-then-merge is the default flow for
   this repo and is pre-authorized for the maintainer's automation.
5. **Update [CHANGELOG.md](CHANGELOG.md)** if your change is user-visible.
   Add a bullet under `## Unreleased`. Phase-shipping PRs also update
   [docs/ROADMAP.md](docs/ROADMAP.md).

## What "shipped" means

Aeroza's roadmap distinguishes "Shipped" from "Up next". Shipped means:

- Code is on `main`.
- Migrations applied (if any).
- Tests cover the new path.
- README + ROADMAP + CHANGELOG reflect the change.
- The relevant route works in the deployed stack
  ([aerozasdk-production.up.railway.app](https://aerozasdk-production.up.railway.app/)
  for the API, [aeroza.vercel.app](https://aeroza.vercel.app/) for the web).

## Code style

Language-specific guidance lives next to the code:

- **Python.** Ruff + mypy strict. PEP 8. Type hints on public surfaces.
  See `pyproject.toml`.
- **TypeScript.** `strict: true` + `noUncheckedIndexedAccess` +
  `exactOptionalPropertyTypes` everywhere (web and SDK). Avoid `any`; use
  `unknown` for untrusted input. See `tsconfig.json`.
- **Files.** Prefer many small focused files (200–400 lines typical, 800
  max) over a few large ones.

## Filing issues

Issues go on GitHub. Tag with `bug`, `feat`, `docs`, or `chore`. For a
bug, include:

- What you expected
- What you saw
- A minimal reproduction
- The output of `make doctor` if it's environment-related

## License

By contributing, you agree your contributions are licensed under the
project's [MIT license](LICENSE).
