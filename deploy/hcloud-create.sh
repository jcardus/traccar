#!/usr/bin/env bash
set -euo pipefail

# Required env vars — set these before running
: "${HCLOUD_TOKEN:?}"


# Optional
SERVER_NAME="${SERVER_NAME:-traccar}"
SERVER_TYPE="${SERVER_TYPE:-cx23}"
LOCATION="${LOCATION:-nbg1}"
SSH_KEY="${SSH_KEY:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Inject credentials into cloud-init
USERDATA_FILE=$(mktemp)
trap 'rm -f "$USERDATA_FILE"' EXIT
export TRACCAR_XML_B64
TRACCAR_XML_B64=$(envsubst < "${SCRIPT_DIR}/traccar.xml" | base64 | tr -d '\n')
envsubst < "${SCRIPT_DIR}/cloud-init.yml" > "$USERDATA_FILE"

cat "$USERDATA_FILE"
# Build hcloud command
HCLOUD_ARGS=(
  server create
  --name "$SERVER_NAME"
  --type "$SERVER_TYPE"
  --image "docker-ce"
  --location "$LOCATION"
  --user-data-from-file "$USERDATA_FILE"
)

if [[ -n "$SSH_KEY" ]]; then
  HCLOUD_ARGS+=(--ssh-key "$SSH_KEY")
fi
echo "run"
echo "${HCLOUD_ARGS[@]}"
hcloud "${HCLOUD_ARGS[@]}"

echo ""
echo "Server '$SERVER_NAME' created. Traccar will be up in ~1-2 minutes."
echo "Point your Cloudflare DNS A record to the server IP shown above (port 8082)."
