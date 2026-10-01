# Morning Handoff

## Finished

- Prepared `dander-connector-salesforce==0.3.2` for Dander through `0.9.x`.
- Preserved plugin API v1 and existing connector runtime behavior.
- Added clean wheel behavior checks to the required distribution job and corrected package docs.

## Try It

Pin Dander and connector versions exactly, then run `dander plugins install` using the README
manifest. For development, run `uv sync --frozen --extra dev` and `uv run pytest`.

## Checks

- Ruff lint/format and strict mypy passed.
- 93 tests passed on the locked stable Dander `0.7.1` environment.
- Fresh wheel installs passed all 93 tests with Dander `0.6.0rc1` and `0.7.1`.
- Fresh wheel installs also passed all 93 tests with public `0.9.0rc20` and the current
  `0.9.0rc32` wheel built from Dander `3bd09301e7becd27792ed7091bdb691edcd9601b`.
- Updated production dependencies passed `pip-audit --strict` with no known vulnerabilities.

## Decisions

- Preserve the existing Dander minimum `0.6.0rc1` and extend only the upper bound to `<0.10`.
- Verify compatibility with existing synthetic tests; no live-provider calls were made.

## Remaining

- Verify the coordinated Dander `0.9.0rc33` release wheel after publication as the final integration check.
- Merge through protected main and verify exact-main CI.
- Tag and publish through the existing workflow so the next Dander catalog can select this version.

## Review First

- `pyproject.toml` and `README.md`
- `.github/workflows/ci.yml`
