#!/usr/bin/env bash
#
# Generate a fresh GPG signing key, publish its public half to the keyservers,
# and set the PGP secrets that .github/workflows/release.yml needs to publish
# parquet to Maven Central with sbt-ci-release.
#
# Follows https://github.com/sbt/sbt-ci-release
# SONATYPE_USERNAME / SONATYPE_PASSWORD are assumed to be set already.
#
# Requirements:
#   - gpg
#   - gh CLI, authenticated: gh auth login
#
# Usage:
#   ./set-release-secrets.sh                 # uses the current repo
#   ./set-release-secrets.sh pityka/parquet  # or name the repo explicitly

set -euo pipefail

REPO="${1:-}"
REPO_ARG=()
if [ -n "$REPO" ]; then
  REPO_ARG=(--repo "$REPO")
fi

read -r -p "Real name for the key: " NAME
read -r -p "Email for the key: " EMAIL

PGP_PASSPHRASE="$(openssl rand -hex 24)"

PARAMS="$(mktemp)"
trap 'rm -f "$PARAMS"' EXIT
cat >"$PARAMS" <<EOF
%echo Generating signing key
Key-Type: RSA
Key-Length: 4096
Subkey-Type: RSA
Subkey-Length: 4096
Name-Real: $NAME
Name-Email: $EMAIL
Expire-Date: 0
Passphrase: $PGP_PASSPHRASE
%commit
%echo done
EOF

STATUS="$(gpg --batch --pinentry-mode loopback --status-fd 1 --gen-key "$PARAMS")"
LONG_ID="$(printf '%s\n' "$STATUS" | awk '/KEY_CREATED/ { print $4 }')"
if [ -z "$LONG_ID" ]; then
  echo "error: key generation did not report a fingerprint" >&2
  exit 1
fi
echo "Generated key $LONG_ID"

echo "Publishing public key to keyservers..."
gpg --keyserver hkp://keyserver.ubuntu.com --send-key "$LONG_ID" \
  || echo "warning: send to keyserver.ubuntu.com failed; retry later" >&2
gpg --keyserver hkp://keys.openpgp.org --send-key "$LONG_ID" \
  || echo "warning: send to keys.openpgp.org failed; retry later" >&2

# Single line: sbt-ci-release fails to decode a line-wrapped secret and then
# falls back to the command-line gpg signer, which fails on CI.
PGP_SECRET="$(gpg --batch --pinentry-mode loopback --passphrase "$PGP_PASSPHRASE" \
  --armor --export-secret-keys "$LONG_ID" | base64 | tr -d '\n')"

printf '%s' "$PGP_PASSPHRASE" | gh secret set PGP_PASSPHRASE "${REPO_ARG[@]}"
printf '%s' "$PGP_SECRET"     | gh secret set PGP_SECRET     "${REPO_ARG[@]}"

echo "Done. Set secrets: PGP_PASSPHRASE, PGP_SECRET"
echo "Key $LONG_ID published. Keyserver propagation can take a few minutes."
