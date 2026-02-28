#!/usr/bin/env bash
# =============================================================================
# ZeroStream - Master Deployment Script
# Orchestrates all deployment steps in correct order
# Usage: bash deployment/deploy_all.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Load environment
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
    echo "✅ Loaded .env from $ROOT_DIR"
else
    echo "❌ .env not found at $ROOT_DIR/.env"
    echo "   Copy .env.example to .env and fill in your values"
    exit 1
fi

# Load generated config (from infra/setup_infra.sh)
if [ -f "$ROOT_DIR/generated_config.env" ]; then
    set -a
    source "$ROOT_DIR/generated_config.env"
    set +a
    echo "✅ Loaded generated_config.env"
else
    echo "⚠️  generated_config.env not found - run 'bash infra/setup_infra.sh' first"
fi

# ── Colours ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

log()     { echo -e "\n${CYAN}[$(date +%H:%M:%S)]${RESET} ${BOLD}$*${RESET}"; }
ok()      { echo -e "  ${GREEN}✅${RESET} $*"; }
warn()    { echo -e "  ${YELLOW}⚠️ ${RESET} $*"; }
err()     { echo -e "  ${RED}❌${RESET} $*"; exit 1; }
divider() { echo -e "  ${CYAN}─────────────────────────────────────────────────${RESET}"; }

STEP=0
TOTAL=4

step() {
    STEP=$((STEP + 1))
    echo ""
    divider
    echo -e "  ${BOLD}Step ${STEP}/${TOTAL}: $*${RESET}"
    divider
}

# ── Header ─────────────────────────────────────────────────────────────────────
clear
echo ""
echo -e "${BOLD}${CYAN}╔═══════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║         ZeroStream - Full Deployment                  ║${RESET}"
echo -e "${BOLD}${CYAN}║   Databricks ZeroBus + Synced Tables + Lakebase       ║${RESET}"
echo -e "${BOLD}${CYAN}╚═══════════════════════════════════════════════════════╝${RESET}"
echo ""
echo -e "  ${BOLD}Configuration:${RESET}"
echo -e "  Delta Table  : ${CYAN}${CATALOG:-not set}.${SCHEMA:-not set}.${TABLE_NAME:-not set}${RESET}"
echo -e "  Lakebase     : ${CYAN}${LAKEBASE_HOST:-not set}${RESET}"
echo -e "  ZeroBus      : ${CYAN}${ZEROBUS_SERVER_ENDPOINT:-not set}${RESET}"
echo -e "  Mobile App   : ${CYAN}${MOBILE_APP:-not set}${RESET}"
echo -e "  Backend App  : ${CYAN}${DASHBOARD_APP:-not set}${RESET}"
echo ""

# ── Step 1: Validate environment ───────────────────────────────────────────────
step "Validate Environment"
bash "${SCRIPT_DIR}/00_setup_environment.sh" \
    && ok "Environment validated" \
    || err "Environment validation failed - fix errors above"

# ── Step 2: Install dependencies ───────────────────────────────────────────────
step "Install Python Dependencies"
bash "${SCRIPT_DIR}/install_dependencies.sh" \
    && ok "Dependencies installed" \
    || err "Dependency installation failed"

# ── Note: Delta tables, Lakebase, and Synced tables are created by infra/setup_infra.sh ──

# ── Step 3: Deploy Databricks Apps ─────────────────────────────────────────────
step "Deploy Databricks Apps"
bash "${SCRIPT_DIR}/03_deploy_apps.sh" \
    && ok "Apps deployed" \
    || err "App deployment failed"

# ── Step 4: Verify deployment ──────────────────────────────────────────────────
step "Verify Deployment"
echo ""
read -r -p "  Run verification checks? [Y/n] " RUN_VERIFY
RUN_VERIFY="${RUN_VERIFY:-Y}"

if [[ "$RUN_VERIFY" =~ ^[Yy]$ ]]; then
    python3 "${SCRIPT_DIR}/verify_setup.py" \
        && ok "All verification checks passed" \
        || warn "Some checks failed - see output above"
else
    ok "Skipping verification"
fi

# ── Final summary ──────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}╔═══════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${GREEN}║         ✅ ZeroStream Deployment Complete!            ║${RESET}"
echo -e "${BOLD}${GREEN}╚═══════════════════════════════════════════════════════╝${RESET}"
echo ""

# Get app URLs
MOBILE_URL=$(databricks apps get "${MOBILE_APP}" \
    --output json 2>/dev/null | \
    python3 -c "import json,sys; print(json.load(sys.stdin).get('url','Check Databricks UI'))" \
    2>/dev/null || echo "Check Databricks Apps UI")

BACKEND_URL=$(databricks apps get "${DASHBOARD_APP}" \
    --output json 2>/dev/null | \
    python3 -c "import json,sys; print(json.load(sys.stdin).get('url','Check Databricks UI'))" \
    2>/dev/null || echo "Check Databricks Apps UI")

echo -e "  📱 Mobile Simulator : ${BOLD}${CYAN}${MOBILE_URL}${RESET}"
echo -e "  📊 Backend Dashboard: ${BOLD}${CYAN}${BACKEND_URL}${RESET}"
echo ""
echo -e "  🗄️  Delta Table      : ${BOLD}${CATALOG}.${SCHEMA}.${TABLE_NAME}${RESET}"
echo -e "  🐘 Lakebase         : ${BOLD}${LAKEBASE_HOST}${RESET}"
echo -e "  ⚡ ZeroBus          : ${BOLD}${ZEROBUS_SERVER_ENDPOINT}${RESET}"
echo ""
echo -e "  ${BOLD}Demo Steps:${RESET}"
echo -e "  1. Open Mobile App → set slider to 10-20 connections"
echo -e "  2. Click START → watch devices stream data"
echo -e "  3. Click any device card → see live sensor readings"
echo -e "  4. Open Backend Dashboard → watch KPIs update in real-time"
echo -e "  5. Click SHOW DATA SERVED BY ZEROBUS → see raw stream"
echo -e "  6. Click any client on map → see location track"
echo ""
echo -e "  ${BOLD}Useful commands:${RESET}"
echo -e "  ${CYAN}# View app logs${RESET}"
echo -e "  databricks apps logs ${MOBILE_APP}"
echo -e "  databricks apps logs ${DASHBOARD_APP}"
echo ""
echo -e "  ${CYAN}# Re-run verification${RESET}"
echo -e "  python3 deployment/verify_setup.py"
echo ""
echo -e "  ${CYAN}# Check Delta table${RESET}"
echo -e "  databricks sql execute \\"
echo -e "    --warehouse-id ${DATABRICKS_WAREHOUSE_ID} \\"
echo -e "    --statement \"SELECT COUNT(*) FROM ${CATALOG}.${SCHEMA}.${TABLE_NAME}\""
echo ""