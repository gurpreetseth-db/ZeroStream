#!/usr/bin/env bash
# =============================================================================
# ZeroStream - Databricks Apps Deployment
# Deploys application code to existing apps.
# Apps MUST be created first via infra setup (infra/create_apps.py).
# This script only uploads code and triggers redeployment.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Load env
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
fi

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

log()  { echo -e "${CYAN}[DEPLOY]${RESET} $*"; }
ok()   { echo -e "${GREEN}[  OK  ]${RESET} $*"; }
warn() { echo -e "${YELLOW}[ WARN ]${RESET} $*"; }
err()  { echo -e "${RED}[ ERR  ]${RESET} $*"; exit 1; }

# ── Validate CLI ───────────────────────────────────────────────────────────────
command -v databricks &>/dev/null || err "databricks CLI not found. Install: pip install databricks-cli"




# ── Helper: check if app exists ───────────────────────────────────────────────
app_exists() {
    local APP_NAME="$1"
    databricks apps get "$APP_NAME" --output json 2>/dev/null | \
    python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print('yes' if d.get('name') else 'no')
except:
    print('no')
" 2>/dev/null || echo "no"
}

# ── Helper: get app URL ────────────────────────────────────────────────────────
get_app_url() {
    local APP_NAME="$1"
    databricks apps get "$APP_NAME" --output json 2>/dev/null | \
    python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print(d.get('url', 'N/A'))
except:
    print('N/A')
" 2>/dev/null || echo "N/A"
}

# ── Helper: wait for app to be ready ──────────────────────────────────────────
wait_for_app() {
    local APP_NAME="$1"
    local MAX_WAIT=120
    local WAITED=0
    local INTERVAL=5

    log "Waiting for $APP_NAME to be ready..."
    while [ $WAITED -lt $MAX_WAIT ]; do
        STATE=$(databricks apps get "$APP_NAME" --output json 2>/dev/null | \
        python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    cs = d.get('compute_status', {})
    if isinstance(cs, dict):
        print(cs.get('state', 'UNKNOWN'))
    else:
        print(str(cs))
except:
    print('UNKNOWN')
" 2>/dev/null || echo "UNKNOWN")

        STATE_UPPER=$(echo "$STATE" | tr '[:lower:]' '[:upper:]')

        if echo "$STATE_UPPER" | grep -qE "ACTIVE|RUNNING|DEPLOYED"; then
            ok "$APP_NAME is $STATE_UPPER"
            return 0
        elif echo "$STATE_UPPER" | grep -qE "ERROR|FAILED|CRASHED"; then
            warn "$APP_NAME state: $STATE_UPPER"
            return 1
        else
            echo -n "."
            sleep $INTERVAL
            WAITED=$((WAITED + INTERVAL))
        fi
    done
    warn "$APP_NAME not ready after ${MAX_WAIT}s (state: $STATE)"
    return 0   # Non-fatal - app may still start
}

# ── Deploy single app ──────────────────────────────────────────────────────────
deploy_app() {
    local APP_NAME="$1"
    local SOURCE_DIR="$2"
    local REMOTE_BASE="/Workspace/zerostream"
    local REMOTE_DIR="${REMOTE_BASE}/${APP_NAME}"

    echo ""
    echo -e "${BOLD}  ── Deploying: ${APP_NAME} ──────────────────────────${RESET}"
    log "Source : $SOURCE_DIR"
    log "Remote : $REMOTE_DIR"

    # ── Step 1: Upload source files ───────────────────────────────────────────
    log "Uploading source files to workspace..."

    # Create remote directory structure
    databricks workspace mkdirs "$REMOTE_DIR" \
         2>/dev/null || true

    # Upload all files
    databricks workspace import-dir \
        "$SOURCE_DIR" \
        "$REMOTE_DIR" \
        --overwrite \
        && ok "Files uploaded to $REMOTE_DIR" \
        || {
            warn "import-dir failed - trying file-by-file upload..."
            _upload_files_individually "$SOURCE_DIR" "$REMOTE_DIR"
        }

    # ── Step 1b: Upload shared config module ──────────────────────────────────
    log "Uploading shared config module..."
    databricks workspace mkdirs "$REMOTE_DIR/config" \
         2>/dev/null || true
    
    databricks workspace import-dir \
        "$ROOT_DIR/config" \
        "$REMOTE_DIR/config" \
        --overwrite \
        && ok "Config module uploaded to $REMOTE_DIR/config" \
        || warn "Config upload failed - app may not start correctly"

    # ── Step 2: Redeploy app (must already exist from infra setup) ─────────────
    EXISTS=$(app_exists "$APP_NAME")

    if [ "$EXISTS" = "yes" ]; then
        log "App '$APP_NAME' exists → deploying updated code..."
        databricks apps deploy "$APP_NAME" \
            --source-code-path "$REMOTE_DIR" \
            && ok "App '$APP_NAME' redeployed successfully" \
            || {
                warn "Deploy command failed - app may still be running previous version"
                warn "Check: databricks apps get $APP_NAME"
         }
    else
        err "App '$APP_NAME' does not exist. Run infra setup first to create the app."
    fi

    # ── Step 3: Wait and get URL ──────────────────────────────────────────────
    wait_for_app "$APP_NAME"
    APP_URL=$(get_app_url "$APP_NAME")
    ok "App URL: ${APP_URL}"

    echo ""
    return 0
}

# ── File-by-file upload fallback ──────────────────────────────────────────────
_upload_files_individually() {
    local LOCAL_DIR="$1"
    local REMOTE_DIR="$2"

    find "$LOCAL_DIR" -type f | while read -r LOCAL_FILE; do
        RELATIVE="${LOCAL_FILE#$LOCAL_DIR/}"
        REMOTE_FILE="${REMOTE_DIR}/${RELATIVE}"
        REMOTE_PARENT=$(dirname "$REMOTE_FILE")

        # Create parent directory
        databricks workspace mkdirs "$REMOTE_PARENT" \
             2>/dev/null || true

        # Determine format
        EXT="${LOCAL_FILE##*.}"
        case "$EXT" in
            py)   FORMAT="SOURCE"; LANG="PYTHON" ;;
            sql)  FORMAT="SOURCE"; LANG="SQL" ;;
            *)    FORMAT="AUTO"; LANG="" ;;
        esac

        if [ -n "$LANG" ]; then
            databricks workspace import \
                "$LOCAL_FILE" \
                "$REMOTE_FILE" \
                --format "$FORMAT" \
                --language "$LANG" \
                --overwrite \
                 2>/dev/null || true
        else
            databricks workspace import \
                "$LOCAL_FILE" \
                "$REMOTE_FILE" \
                --overwrite \
                 2>/dev/null || true
        fi
    done
    ok "Files uploaded individually"
}

# ── Main deployment ────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}╔═══════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║   ZeroStream - Databricks Apps Deployment         ║${RESET}"
echo -e "${BOLD}${CYAN}╚═══════════════════════════════════════════════════╝${RESET}"

# Deploy mobile app
deploy_app \
    "${MOBILE_APP:-zerostream-mobile-app}" \
    "${ROOT_DIR}/mobile_app"

# Deploy backend app
deploy_app \
    "${DASHBOARD_APP:-zerostream-app}" \
    "${ROOT_DIR}/dashboard_app"

# ── Final summary ──────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}╔═══════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${GREEN}║   ✅ Apps Deployed Successfully                   ║${RESET}"
echo -e "${BOLD}${GREEN}╚═══════════════════════════════════════════════════╝${RESET}"
echo ""

MOBILE_URL=$(get_app_url "${MOBILE_APP:-zerostream-mobile-app}")
BACKEND_URL=$(get_app_url "${DASHBOARD_APP:-zerostream-app}")

echo -e "  📱 Mobile App  : ${BOLD}${CYAN}${MOBILE_URL}${RESET}"
echo -e "  📊 Backend App : ${BOLD}${CYAN}${BACKEND_URL}${RESET}"
echo ""
echo -e "  To view app logs:"
echo -e "  ${CYAN}databricks apps logs ${MOBILE_APP}${RESET}"
echo -e "  ${CYAN}databricks apps logs ${DASHBOARD_APP}${RESET}"
echo ""