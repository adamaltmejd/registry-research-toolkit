#!/usr/bin/env bash
# Fly registry prune (#290), run by container-build.yml's `prune-registry` (the global
# app) and steward-deploy.yml's `prune` (steward apps): keep the newest KEEP tags of
# FLY_APP as rollback handles and delete older manifests. Never deletes DEPLOYED_TAG,
# a tag newer than it (it may belong to a run still deploying) or a digest a kept tag
# shares (deleting a manifest removes every tag pointing at it). Ordering is by the
# image config's `.created`: buildx pushes OCI indexes, whose real image manifest is
# the entry with a known platform (the other is the provenance attestation). Fly's
# registry accepts the standard v2 manifest DELETE. Env: FLY_API_TOKEN, FLY_APP,
# DEPLOYED_TAG.
set -euo pipefail
: "${FLY_API_TOKEN:?}" "${FLY_APP:?}" "${DEPLOYED_TAG:?}"
REPO_URL="https://registry.fly.io/v2/$FLY_APP"
ACCEPT='Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'
KEEP=10
auth="x:$FLY_API_TOKEN"
listing=""
for t in $(curl -fsS -u "$auth" "$REPO_URL/tags/list" | jq -r '.tags[]'); do
	top_digest=$(curl -fsSI -u "$auth" -H "$ACCEPT" "$REPO_URL/manifests/$t" |
		tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest" {print $2}')
	manifest=$(curl -fsS -u "$auth" -H "$ACCEPT" "$REPO_URL/manifests/$t")
	if echo "$manifest" | jq -e '.manifests' >/dev/null; then
		img=$(echo "$manifest" | jq -r '[.manifests[] | select(.platform.architecture != "unknown")][0].digest')
		cfg=$(curl -fsS -u "$auth" -H "$ACCEPT" "$REPO_URL/manifests/$img" | jq -r '.config.digest')
	else
		cfg=$(echo "$manifest" | jq -r '.config.digest')
	fi
	created=$(curl -fsSL -u "$auth" "$REPO_URL/blobs/$cfg" | jq -r '.created')
	listing+="$created $t $top_digest"$'\n'
done
kept=$(printf '%s' "$listing" | sort -r | head -n "$KEEP")
echo "Keeping $(printf '%s\n' "$kept" | wc -l | tr -d ' ') tags:"
printf '%s\n' "$kept"
kept_digests=$(printf '%s\n' "$kept" | awk '{print $3}')
# If the deployed tag is not listed, every tag counts as newer and nothing is pruned.
deployed_created=$(printf '%s' "$listing" | awk -v t="$DEPLOYED_TAG" '$2 == t {print $1}')
printf '%s' "$listing" | sort -r | tail -n "+$((KEEP + 1))" |
	while read -r created tag digest; do
		if [ "$tag" = "$DEPLOYED_TAG" ]; then
			echo "skip $tag (just deployed)"
		elif [ -z "$deployed_created" ] || [[ "$created" > "$deployed_created" ]]; then
			echo "skip $tag (newer than the deployed image: may be deploying)"
		elif printf '%s\n' "$kept_digests" | grep -qF "$digest"; then
			echo "skip $tag (digest shared with a kept tag)"
		else
			echo "prune $tag"
			curl -fsS -X DELETE -u "$auth" "$REPO_URL/manifests/$digest" ||
				echo "::warning::failed to delete $tag"
		fi
	done
