#!/usr/bin/env bash
# =============================================================================
# ZeroStream - Environment Validation Script
# asyncpg is OPTIONAL - pg8000 is the guaranteed fallback
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Load env
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
else
    echo "❌ .env file not found at $ROOT_DIR/.env"
    echo "   Copy .env.example to .env and fill in your values"
    exit 1
fi

# ── Colours ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

log()  { echo -e "${CYAN}[CHECK]${RESET}  $*"; }
ok()   { echo -e "${GREEN}[  OK ]${RESET}  $*"; }
warn() { echo -e "${YELLOW}[ WARN]${RESET}  $*"; WARNED=$((WARNED+1)); }
fail() { echo -e "${RED}[ FAIL]${RESET}  $*"; FAILED=$((FAILED+1)); }

FAILED=0
WARNED=0

echo ""
echo -e "${BOLD}${CYAN}╔═══════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║   ZeroStream - Pre-Deployment Validation          ║${RESET}"
echo -e "${BOLD}${CYAN}╚═══════════════════════════════════════════════════╝${RESET}"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1: System Tools
# ══════════════════════════════════════════════════════════════════════════════
echo -e "${BOLD}  System Tools${RESET}"
echo -e "  ─────────────────────────────────────────────────"

# Python
log "Python version..."
if command -v python3 &>/dev/null; then
    PY_VER=$(python3 -c \
        "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')")
    PY_MAJ=$(python3 -c "import sys; print(sys.version_info.major)")
    PY_MIN=$(python3 -c "import sys; print(sys.version_info.minor)")
    if [ "$PY_MAJ" -ge 3 ] && [ "$PY_MIN" -ge 9 ]; then
        ok "Python $PY_VER"
    else
        fail "Python $PY_VER — 3.9+ required"
    fi
else
    fail "python3 not found"
fi

# pip
log "pip..."
if python3 -m pip --version &>/dev/null 2>&1; then
    PIP_VER=$(python3 -m pip --version | awk '{print $2}')
    ok "pip $PIP_VER"
else
    fail "pip not found — run: python3 -m ensurepip"
fi

# Databricks CLI
log "Databricks CLI..."
if command -v databricks &>/dev/null; then
    DB_VER=$(databricks --version 2>&1 | head -1 || echo "unknown")
    ok "Databricks CLI: $DB_VER"
else
    fail "Databricks CLI not found — install: pip install databricks-cli"
fi

# curl (optional)
log "curl..."
if command -v curl &>/dev/null; then
    ok "curl available"
else
    warn "curl not found — connectivity checks will be skipped"
fi

# psql (optional)
log "psql..."
if command -v psql &>/dev/null; then
    ok "psql: $(psql --version | head -1)"
else
    warn "psql not found — Python fallback will be used (no action needed)"
fi

echo ""

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: Python Packages
# ══════════════════════════════════════════════════════════════════════════════
echo -e "${BOLD}  Python Packages${RESET}"
echo -e "  ─────────────────────────────────────────────────"

# Helper to check one package
check_pkg() {
    local PKG="$1"
    local IMPORT="${2:-$1}"
    python3 -c "import $IMPORT" 2>/dev/null
    return $?
}

get_ver() {
    local IMPORT="$1"
    python3 -c "
import importlib
try:
    m = importlib.import_module('$IMPORT')
    print(getattr(m, '__version__', 'installed'))
except Exception:
    print('installed')
" 2>/dev/null || echo "installed"
}

# ── Required packages ─────────────────────────────────────────────────────────
echo -e "  ${BOLD}Required packages:${RESET}"

REQUIRED_CHECKS=(
    "fastapi:fastapi"
    "uvicorn:uvicorn"
    "jinja2:jinja2"
    "databricks-sdk:databricks.sdk"
    "httpx:httpx"
    "python-dotenv:dotenv"
    "aiofiles:aiofiles"
)

for ENTRY in "${REQUIRED_CHECKS[@]}"; do
    PKG="${ENTRY%%:*}"
    IMPORT="${ENTRY##*:}"
    log "$PKG..."
    if python3 -c "import $IMPORT" 2>/dev/null; then
        VER=$(get_ver "$IMPORT")
        ok "$PKG ($VER)"
    else
        fail "$PKG not installed"
        echo "         Fix: bash deployment/install_dependencies.sh"
    fi
done

echo ""

# ── Optional packages (warn only — NEVER fail) ────────────────────────────────
echo -e "  ${BOLD}Optional packages (warn only):${RESET}"

# pydantic
log "pydantic (optional — config uses dataclasses)..."
if python3 -c "import pydantic" 2>/dev/null; then
    VER=$(get_ver "pydantic")
    ok "pydantic $VER"
else
    warn "pydantic not installed — config/settings.py uses dataclasses (no impact)"
fi

# asyncpg
log "asyncpg (optional — pg8000 is fallback)..."
if python3 -c "import asyncpg" 2>/dev/null; then
    VER=$(get_ver "asyncpg")
    ok "asyncpg $VER (async PostgreSQL driver)"
else
    warn "asyncpg not installed — pg8000 will handle async ops (fully supported)"
fi

# pg8000
log "pg8000 (pure Python PostgreSQL — preferred fallback)..."
if python3 -c "import pg8000" 2>/dev/null; then
    VER=$(get_ver "pg8000")
    ok "pg8000 $VER (pure Python — zero build deps)"
else
    warn "pg8000 not installed — install: pip install pg8000"
fi

# psycopg2
log "psycopg2 (optional sync driver)..."
if python3 -c "import psycopg2" 2>/dev/null; then
    VER=$(get_ver "psycopg2")
    ok "psycopg2 $VER"
elif python3 -c "import psycopg" 2>/dev/null; then
    VER=$(get_ver "psycopg")
    ok "psycopg3 $VER"
else
    warn "psycopg2/psycopg3 not installed — pg8000 handles sync ops"
fi

echo ""

# ── At least ONE PostgreSQL driver must exist ─────────────────────────────────
log "PostgreSQL driver availability..."
PG_FOUND=false
PG_NAME=""
for DRV in asyncpg pg8000 psycopg2 psycopg; do
    if python3 -c "import $DRV" 2>/dev/null; then
        PG_FOUND=true
        PG_NAME="$DRV"
        break
    fi
done

if [ "$PG_FOUND" = true ]; then
    ok "PostgreSQL driver available: $PG_NAME"
else
    fail "No PostgreSQL driver found"
    echo "         Fix: pip install pg8000"
fi

echo ""

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3: Environment Variables
# ══════════════════════════════════════════════════════════════════════════════
echo -e "${BOLD}  Environment Variables${RESET}"
echo -e "  ─────────────────────────────────────────────────"

check_var() {
    local NAME="$1"
    local IS_SECRET="${2:-false}"
    local IS_REQUIRED="${3:-true}"
    local VALUE="${!NAME:-}"

    if [ -n "$VALUE" ]; then
        if [ "$IS_SECRET" = "true" ]; then
            DISPLAY="***set (${#VALUE} chars)***"
        else
            DISPLAY="$VALUE"
        fi
        ok "$NAME = $DISPLAY"
    elif [ "$IS_REQUIRED" = "true" ]; then
        fail "$NAME = NOT SET"
    else
        warn "$NAME = NOT SET (optional)"
    fi
}

echo -e "  ${BOLD}Databricks:${RESET}"
check_var "DATABRICKS_HOST"         "false" "true"
check_var "DATABRICKS_TOKEN"        "true"  "true"
check_var "DATABRICKS_WAREHOUSE_ID" "false" "true"
echo ""

echo -e "  ${BOLD}Delta Table:${RESET}"
check_var "CATALOG"    "false" "true"
check_var "SCHEMA"     "false" "true"
check_var "TABLE_NAME" "false" "true"
echo ""

echo -e "  ${BOLD}ZeroBus:${RESET}"
check_var "ZEROBUS_SERVER_ENDPOINT" "false" "true"
check_var "ZEROBUS_CLIENT_ID"       "false" "true"
check_var "ZEROBUS_CLIENT_SECRET"   "true"  "true"
check_var "STREAM_INTERVAL_MS"      "false" "false"
echo ""

echo -e "  ${BOLD}Lakebase:${RESET}"
check_var "LAKEBASE_INSTANCE"  "false" "true"
check_var "LAKEBASE_HOST"      "false" "true"
check_var "LAKEBASE_PORT"      "false" "false"
#check_var "LAKEBASE_USER"      "false" "true"
#check_var "LAKEBASE_PASSWORD"  "true"  "true"
check_var "LAKEBASE_DATABASES" "false" "true"
check_var "LAKEBASE_SCHEMA"    "false" "false"
echo ""

echo -e "  ${BOLD}Apps:${RESET}"
check_var "MOBILE_APP"  "false" "true"
check_var "DASHBOARD_APP" "false" "true"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4: Network Connectivity
# ══════════════════════════════════════════════════════════════════════════════
echo -e "${BOLD}  Network Connectivity${RESET}"
echo -e "  ─────────────────────────────────────────────────"

if ! command -v curl &>/dev/null; then
    warn "curl not available — skipping all connectivity checks"
else
    # Databricks workspace
    log "Databricks workspace..."
    if [ -n "${DATABRICKS_HOST:-}" ] && [ -n "${DATABRICKS_TOKEN:-}" ]; then
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            --max-time 10 \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            "${DATABRICKS_HOST}api/2.0/clusters/list" \
            2>/dev/null || echo "000")
        case "$HTTP_CODE" in
            200)     ok "Databricks workspace reachable (HTTP $HTTP_CODE)" ;;
            401|403) warn "Databricks reachable but auth issue (HTTP $HTTP_CODE)" ;;
            000)     warn "Databricks unreachable — check DATABRICKS_HOST" ;;
            *)       warn "Databricks returned HTTP $HTTP_CODE" ;;
        esac
    else
        warn "DATABRICKS_HOST or TOKEN not set — skipping"
    fi

    # ZeroBus
    log "ZeroBus endpoint..."
    if [ -n "${ZEROBUS_SERVER_ENDPOINT:-}" ]; then
        ZB_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            --max-time 10 \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN:-}" \
            "https://${ZEROBUS_SERVER_ENDPOINT}/api/2.0/zerobus/topics" \
            2>/dev/null || echo "000")
        case "$ZB_CODE" in
            200|404) ok "ZeroBus endpoint reachable (HTTP $ZB_CODE)" ;;
            401|403) warn "ZeroBus reachable but auth issue (HTTP $ZB_CODE)" ;;
            000)     warn "ZeroBus unreachable — check ZEROBUS_SERVER_ENDPOINT" ;;
            *)       warn "ZeroBus returned HTTP $ZB_CODE" ;;
        esac
    else
        warn "ZEROBUS_SERVER_ENDPOINT not set — skipping"
    fi

    # Lakebase
    log "Lakebase PostgreSQL..."
    if [ -n "${LAKEBASE_HOST:-}" ] && \
       [ -n "${LAKEBASE_USER:-}" ] && \
       [ -n "${LAKEBASE_PASSWORD:-}" ] && \
       [ -n "${LAKEBASE_DATABASES:-}" ]; then

        PG_RESULT=$(python3 -c "
import sys

def try_asyncpg():
    try:
        import asyncio, asyncpg
        async def _t():
            c = await asyncpg.connect(
                host='${LAKEBASE_HOST}',
                port=${LAKEBASE_PORT:-5432},
                user='${LAKEBASE_USER}',
                password='${LAKEBASE_PASSWORD}',
                database='${LAKEBASE_DATABASES}',
                ssl='require',
                timeout=10,
            )
            await c.close()
        asyncio.run(_t())
        print('OK:asyncpg')
        return True
    except ImportError:
        return False
    except Exception as e:
        print(f'FAIL:{e}')
        return True

def try_pg8000():
    try:
        import pg8000, ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        c = pg8000.connect(
            host='${LAKEBASE_HOST}',
            port=${LAKEBASE_PORT:-5432},
            user='${LAKEBASE_USER}',
            password='${LAKEBASE_PASSWORD}',
            database='${LAKEBASE_DATABASES}',
            ssl_context=ctx,
        )
        c.close()
        print('OK:pg8000')
        return True
    except ImportError:
        return False
    except Exception as e:
        print(f'FAIL:{e}')
        return True

if not try_asyncpg():
    if not try_pg8000():
        print('FAIL:no_driver')
" 2>/dev/null || echo "FAIL:script_error")

        if echo "$PG_RESULT" | grep -q "^OK:"; then
            DRV=$(echo "$PG_RESULT" | sed 's/^OK://')
            ok "Lakebase connected via $DRV"
        else
            ERR=$(echo "$PG_RESULT" | sed 's/^FAIL://')
            warn "Lakebase issue: $ERR"
        fi
    else
        warn "Lakebase vars incomplete — skipping connectivity check"
    fi
fi

echo ""

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5: Summary
# ══════════════════════════════════════════════════════════════════════════════
echo -e "${BOLD}  Deployment Configuration${RESET}"
echo -e "  ─────────────────────────────────────────────────"
echo -e "  Delta Table  : ${BOLD}${CATALOG:-?}.${SCHEMA:-?}.${TABLE_NAME:-?}${RESET}"
echo -e "  Lakebase     : ${BOLD}${LAKEBASE_HOST:-?}:${LAKEBASE_PORT:-5432}/${LAKEBASE_DATABASES:-?}${RESET}"
echo -e "  ZeroBus      : ${BOLD}${ZEROBUS_SERVER_ENDPOINT:-?}${RESET}"
echo -e "  Mobile App   : ${BOLD}${MOBILE_APP:-?}${RESET}"
echo -e "  Backend App  : ${BOLD}${DASHBOARD_APP:-?}${RESET}"
echo -e "  Stream Rate  : ${BOLD}${STREAM_INTERVAL_MS:-5000}ms${RESET}"
echo -e "  Active Window: ${BOLD}${ACTIVE_WINDOW_SECONDS:-5}s${RESET}"
echo ""

# ── Final result ───────────────────────────────────────────────────────────────
echo -e "${BOLD}${CYAN}╔═══════════════════════════════════════════════════╗${RESET}"
if [ "$FAILED" -eq 0 ]; then
    if [ "$WARNED" -gt 0 ]; then
        echo -e "${BOLD}${GREEN}║  ✅ Validation passed ($WARNED warning(s))          ║${RESET}"
    else
        echo -e "${BOLD}${GREEN}║  ✅ Validation passed — all checks OK             ║${RESET}"
    fi
    echo -e "${BOLD}${GREEN}╚═══════════════════════════════════════════════════╝${RESET}"
    echo ""
    if [ "$WARNED" -gt 0 ]; then
        echo -e "  ${YELLOW}Warnings are non-critical — deployment will proceed.${RESET}"
    fi
    echo ""
    exit 0
else
    echo -e "${BOLD}${RED}║  ❌ Validation failed ($FAILED error(s), $WARNED warning(s))${RESET}"
    echo -e "${BOLD}${RED}╚═══════════════════════════════════════════════════╝${RESET}"
    echo ""
    echo -e "  Fix the ${RED}${BOLD}${FAILED} error(s)${RESET} above then re-run:"
    echo -e "  ${CYAN}bash deployment/deploy_all.sh"
    echo ""
    exit 1
fi
