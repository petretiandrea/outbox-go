#!/usr/bin/env bash
# Publish the pkg/outbox/<root_tag> tag matching an existing root release tag.
# Usage: tag-nested-module.sh <root_tag>
# Env:   PUSH_REMOTE  remote name or URL to push to (default: origin)
set -euo pipefail

root_tag="${1:?usage: tag-nested-module.sh <root_tag>}"
remote="${PUSH_REMOTE:-origin}"
nested_tag="pkg/outbox/${root_tag}"

case "${root_tag}" in
  v[0-9]*) ;;
  *) echo "root_tag must be a semantic root tag such as v1.7.1" >&2; exit 1 ;;
esac

release_commit="$(git rev-list -n 1 "${root_tag}")"

test "$(git show "${release_commit}:pkg/outbox/go.mod" | head -n 1)" = "module github.com/petretiandrea/outbox-go/pkg/outbox"

if git ls-remote --exit-code --tags origin "refs/tags/${nested_tag}" >/dev/null 2>&1; then
  remote_commit="$(git ls-remote origin "refs/tags/${nested_tag}^{}" | awk '{print $1}')"
  if [ -z "${remote_commit}" ]; then
    remote_commit="$(git ls-remote --refs origin "refs/tags/${nested_tag}" | awk '{print $1}')"
  fi
  test "${remote_commit}" = "${release_commit}"
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git tag -a "${nested_tag}" "${release_commit}" -m "Release ${nested_tag}"
git push "${remote}" "refs/tags/${nested_tag}"
