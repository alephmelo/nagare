---
trigger: always_on
---

# Releasing

Tag-driven releases via GoReleaser + GitHub Actions.

## Steps

1. Ensure `main` has all desired changes merged.
2. Tag the release: `git tag v<VERSION>`
3. Push the tag: `git push origin v<VERSION>`

GitHub Actions handles the rest (cross-compiled builds, changelog, GitHub Release).

## Versioning

- Manual semver — no automated version bumping.
- Version is derived from the Git tag at build time.
- Use conventional commit prefixes (`feat:`, `fix:`, etc.) for changelog grouping.
