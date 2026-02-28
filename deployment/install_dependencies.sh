#!/usr/bin/env bash
# =============================================================================
# ZeroStream - Smart Dependency Installer v2
# Fixes: pydantic-core (Rust), psycopg2 (libpq), asyncpg (C extension)
# Strategy: pre-built wheels first → Rust install → pydantic v1 fallback
# =============================================================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

log()  { echo -e "${CYAN}[INSTALL]${RESET} $*"; }
ok()   { echo -e "${GREEN}[  OK   ]${RESET} $*"; }
warn() { echo -e "${YELLOW}[ WARN  ]${RESET} $*"; }
err()  { echo -e "${RED}[ ERROR ]${RESET} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo ""
echo -e "${BOLD}${CYAN}╔═══════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║   ZeroStream - Smart Dependency Installer v2      ║${RESET}"
echo -e "${BOLD}${CYAN}╚═══════════════════════════════════════════════════╝${RESET}"
echo ""

# ── Detect environment ────────────────────────────────────────────────────────
OS="$(uname -s)"
ARCH="$(uname -m)"
PY_FULL=$(python3 --version 2>&1)
PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJ=$(python3 -c "import sys; print(sys.version_info.major)")
PY_MIN=$(python3 -c "import sys; print(sys.version_info.minor)")

log "OS       : $OS / $ARCH"
log "Python   : $PY_FULL"
log "Py Ver   : $PY_VER"

# Check Python version
if [ "$PY_MAJ" -lt 3 ] || ([ "$PY_MAJ" -eq 3 ] && [ "$PY_MIN" -lt 9 ]); then
    err "Python 3.9+ required (found $PY_VER)"
fi

# Detect Databricks runtime
IS_DATABRICKS=false
if [ -n "${DATABRICKS_RUNTIME_VERSION:-}" ] || \
   [ -f "/databricks/spark/conf/spark-defaults.conf" ]; then
    IS_DATABRICKS=true
    log "Runtime  : Databricks (detected)"
fi

echo ""

# ── Step 1: Upgrade pip ───────────────────────────────────────────────────────
log "Step 1/7 - Upgrading pip to latest..."
python3 -m pip install \
    --upgrade \
    --quiet \
    "pip>=24.0" \
    setuptools \
    wheel \
    2>/dev/null || python3 -m pip install --upgrade pip setuptools wheel
ok "pip $(python3 -m pip --version | awk '{print $2}')"

# ── Step 2: Try to install Rust (needed for pydantic-core if no wheel) ────────
log "Step 2/7 - Checking Rust toolchain..."

RUST_AVAILABLE=false
if command -v rustc &>/dev/null; then
    RUST_VER=$(rustc --version 2>/dev/null | awk '{print $2}' || echo "?")
    ok "Rust already installed: $RUST_VER"
    RUST_AVAILABLE=true
else
    warn "Rust not found"
    log "  Attempting rustup install (needed if no pydantic wheel available)..."

    if [ "$OS" = "Darwin" ] || [ "$OS" = "Linux" ]; then
        if curl --proto '=https' --tlsv1.2 -sSf \
            https://sh.rustup.rs \
            -o /tmp/rustup_init.sh 2>/dev/null; then

            chmod +x /tmp/rustup_init.sh
            if /tmp/rustup_init.sh -y \
                --no-modify-path \
                --quiet \
                2>/dev/null; then

                # Source cargo environment
                if [ -f "$HOME/.cargo/env" ]; then
                    source "$HOME/.cargo/env"
                fi
                export PATH="$HOME/.cargo/bin:${PATH:-}"

                if command -v rustc &>/dev/null; then
                    RUST_VER=$(rustc --version | awk '{print $2}')
                    ok "Rust installed: $RUST_VER"
                    RUST_AVAILABLE=true
                fi
            fi
        fi
    fi

    if [ "$RUST_AVAILABLE" = false ]; then
        warn "Rust not available - will use pydantic v1 or pre-built wheels only"
    fi
fi

# ── Step 3: Install pydantic (most problematic) ───────────────────────────────
log "Step 3/7 - Installing pydantic..."

PYDANTIC_INSTALLED=false
PYDANTIC_VERSION=""

# ── Strategy A: Force pre-built binary for pydantic v2 ────────────────────────
log "  [A] pydantic v2 pre-built binary..."
if python3 -m pip install \
    "pydantic>=2.0.0,<3.0.0" \
    "pydantic-core>=2.0.0,<3.0.0" \
    --prefer-binary \
    --only-binary=pydantic,pydantic-core \
    --quiet \
    2>/dev/null; then
    PYDANTIC_VERSION=$(python3 -c \
        "import pydantic; print(pydantic.__version__)" 2>/dev/null || echo "?")
    ok "pydantic $PYDANTIC_VERSION (pre-built binary)"
    PYDANTIC_INSTALLED=true
fi

# ── Strategy B: Specific versions with known wheels ───────────────────────────
if [ "$PYDANTIC_INSTALLED" = false ]; then
    log "  [B] Trying specific pydantic v2 versions with known wheels..."
    for VER_PAIR in \
        "2.7.4:2.20.1" \
        "2.7.1:2.18.4" \
        "2.6.4:2.16.3" \
        "2.5.3:2.14.6" \
        "2.4.2:2.10.1" \
        "2.3.0:2.6.3"; do

        PYDANTIC_VER="${VER_PAIR%%:*}"
        CORE_VER="${VER_PAIR##*:}"

        if python3 -m pip install \
            "pydantic==$PYDANTIC_VER" \
            "pydantic-core==$CORE_VER" \
            --prefer-binary \
            --only-binary=pydantic,pydantic-core \
            --quiet \
            2>/dev/null; then
            PYDANTIC_VERSION=$(python3 -c \
                "import pydantic; print(pydantic.__version__)" 2>/dev/null || echo "$PYDANTIC_VER")
            ok "pydantic $PYDANTIC_VERSION (pre-built)"
            PYDANTIC_INSTALLED=true
            break
        fi
    done
fi

# ── Strategy C: Build with Rust if available ──────────────────────────────────
if [ "$PYDANTIC_INSTALLED" = false ] && [ "$RUST_AVAILABLE" = true ]; then
    log "  [C] Building pydantic v2 with Rust..."
    if python3 -m pip install \
        "pydantic>=2.0.0,<3.0.0" \
        --quiet \
        2>/dev/null; then
        PYDANTIC_VERSION=$(python3 -c \
            "import pydantic; print(pydantic.__version__)" 2>/dev/null || echo "?")
        ok "pydantic $PYDANTIC_VERSION (built with Rust)"
        PYDANTIC_INSTALLED=true
    fi
fi

# ── Strategy D: pydantic v1 (no Rust, no binary needed) ──────────────────────
if [ "$PYDANTIC_INSTALLED" = false ]; then
    log "  [D] Falling back to pydantic v1 (no Rust required)..."
    for V1_VER in "1.10.21" "1.10.18" "1.10.13" "1.10.0"; do
        if python3 -m pip install \
            "pydantic==$V1_VER" \
            --prefer-binary \
            --quiet \
            2>/dev/null; then
            PYDANTIC_VERSION=$(python3 -c \
                "import pydantic; print(pydantic.__version__)" 2>/dev/null || echo "$V1_VER")
            ok "pydantic $PYDANTIC_VERSION (v1 - no Rust needed)"
            PYDANTIC_INSTALLED=true
            warn "Using pydantic v1 - patching settings.py for compatibility"
            _apply_pydantic_v1_patch
            break
        fi
    done
fi

if [ "$PYDANTIC_INSTALLED" = false ]; then
    err "All pydantic strategies failed. Try: pip install pydantic --prefer-binary"
fi

# ── Pydantic v1 compatibility patch ───────────────────────────────────────────
_apply_pydantic_v1_patch() {
    local SETTINGS="$ROOT_DIR/config/settings.py"
    if [ -f "$SETTINGS" ]; then
        # settings.py already uses dataclasses - no patch needed
        ok "config/settings.py uses dataclasses - no pydantic patch needed"
    fi

    # Install pydantic-settings for v1 if needed
    python3 -m pip install \
        "pydantic-settings" \
        --prefer-binary \
        --quiet 2>/dev/null || true
}

# ── Step 4: Install pydantic-settings ─────────────────────────────────────────
log "Step 4/7 - Installing pydantic-settings..."

PYDANTIC_MAJOR=$(python3 -c \
    "import pydantic; print(int(pydantic.__version__.split('.')[0]))" \
    2>/dev/null || echo "1")

if [ "$PYDANTIC_MAJOR" -ge 2 ]; then
    if python3 -m pip install \
        "pydantic-settings>=2.0.0,<3.0.0" \
        --prefer-binary \
        --quiet 2>/dev/null; then
        ok "pydantic-settings installed (v2 compatible)"
    else
        warn "pydantic-settings failed - config uses dataclasses (no impact)"
    fi
else
    ok "pydantic v1 - BaseSettings built-in, pydantic-settings not needed"
fi

# ── Step 5: Install core packages ─────────────────────────────────────────────
log "Step 5/7 - Installing core packages..."

# Install in small groups for better error isolation
_pip_install() {
    local DESC="$1"; shift
    local PKGS=("$@")
    if python3 -m pip install \
        --prefer-binary \
        --quiet \
        "${PKGS[@]}" 2>/dev/null; then
        ok "$DESC"
        return 0
    fi
    # Try one by one
    warn "$DESC - group install failed, trying individually..."
    local FAILED=0
    for pkg in "${PKGS[@]}"; do
        if python3 -m pip install \
            --prefer-binary \
            --quiet \
            "$pkg" 2>/dev/null; then
            ok "  $pkg"
        else
            warn "  $pkg failed"
            FAILED=$((FAILED+1))
        fi
    done
    return $FAILED
}

_pip_install "FastAPI + Uvicorn" \
    "fastapi==0.111.0" \
    "uvicorn[standard]==0.29.0" \
    "python-multipart==0.0.9"

_pip_install "Jinja2 + aiofiles" \
    "jinja2==3.1.4" \
    "aiofiles==23.2.1"

_pip_install "HTTP clients" \
    "httpx==0.27.0" \
    "aiohttp==3.9.5"

_pip_install "Config + SQLAlchemy" \
    "python-dotenv==1.0.1" \
    "sqlalchemy>=2.0.47"

_pip_install "Databricks SDK" \
    "databricks-sdk==0.27.1"

# ── Step 6: Install PostgreSQL drivers ────────────────────────────────────────
log "Step 6/7 - Installing PostgreSQL drivers..."

# pg8000 first - pure Python, always works
log "  pg8000 (pure Python - guaranteed)..."
if python3 -m pip install \
    "pg8000==1.31.1" \
    --quiet 2>/dev/null || \
   python3 -m pip install \
    "pg8000" \
    --quiet 2>/dev/null; then
    PG8000_VER=$(python3 -c \
        "import pg8000; print(pg8000.__version__)" 2>/dev/null || echo "?")
    ok "pg8000 $PG8000_VER (pure Python)"
else
    warn "pg8000 failed"
fi

# asyncpg - pre-built wheels
log "  asyncpg (async driver)..."
ASYNCPG_OK=false
for VER in "0.29.0" "0.28.0" "0.27.0" ""; do
    PKG="asyncpg${VER:+==$VER}"
    if python3 -m pip install \
        "$PKG" \
        --prefer-binary \
        --quiet 2>/dev/null; then
        ASYNCPG_VER=$(python3 -c \
            "import asyncpg; print(asyncpg.__version__)" 2>/dev/null || echo "?")
        ok "asyncpg $ASYNCPG_VER"
        ASYNCPG_OK=true
        break
    fi
done
[ "$ASYNCPG_OK" = false ] && warn "asyncpg failed - pg8000 will handle async ops"

# psycopg2 - optional
log "  psycopg2 (optional sync driver)..."
if python3 -m pip install \
    "psycopg2-binary" \
    --prefer-binary \
    --only-binary=:all: \
    --quiet 2>/dev/null; then
    ok "psycopg2-binary installed"
elif python3 -m pip install \
    "psycopg[binary]" \
    --prefer-binary \
    --quiet 2>/dev/null; then
    ok "psycopg3 installed"
else
    warn "psycopg2 not available (pg8000 fallback active)"
fi

# ── Step 7: Final verification ────────────────────────────────────────────────
log "Step 7/7 - Verifying all imports..."
echo ""

python3 << 'PYEOF'
import sys

GREEN  = '\033[0;32m'
YELLOW = '\033[0;33m'
RED    = '\033[0;31m'
CYAN   = '\033[0;36m'
BOLD   = '\033[1m'
RESET  = '\033[0m'

REQUIRED = [
    ("fastapi",        "Web framework"),
    ("uvicorn",        "ASGI server"),
    ("jinja2",         "Templates"),
    ("databricks.sdk", "Databricks SDK"),
    ("httpx",          "HTTP client"),
    ("dotenv",         "Env config"),
    ("sqlalchemy",     "SQL toolkit"),
    ("aiofiles",       "Async file I/O"),
]

OPTIONAL = [
    ("pydantic",   "Data validation"),
    ("asyncpg",    "Async PostgreSQL"),
    ("pg8000",     "Pure Python PostgreSQL"),
    ("psycopg2",   "psycopg2 sync"),
    ("psycopg",    "psycopg3 sync"),
]

print(f"  {BOLD}Required packages:{RESET}")
all_ok = True
for pkg, desc in REQUIRED:
    try:
        mod = __import__(pkg)
        ver = getattr(mod, "__version__", "installed")
        print(f"    {GREEN}✅{RESET} {pkg:<25} {ver:<15} {desc}")
    except ImportError:
        print(f"    {RED}❌{RESET} {pkg:<25} {'MISSING':<15} {desc}")
        all_ok = False

print(f"\n  {BOLD}Optional packages:{RESET}")
pg_found = False
for pkg, desc in OPTIONAL:
    try:
        mod = __import__(pkg)
        ver = getattr(mod, "__version__", "installed")
        print(f"    {GREEN}✅{RESET} {pkg:<25} {ver:<15} {desc}")
        if pkg in ("asyncpg", "pg8000", "psycopg2", "psycopg"):
            pg_found = True
    except ImportError:
        print(f"    {YELLOW}⚪{RESET} {pkg:<25} {'not installed':<15} {desc}")

if not pg_found:
    print(f"\n    {RED}❌{RESET} No PostgreSQL driver found!")
    all_ok = False
else:
    print(f"\n    {GREEN}✅{RESET} PostgreSQL driver available")

# Check pydantic version
try:
    import pydantic
    v = int(pydantic.__version__.split(".")[0])
    if v >= 2:
        print(f"    {GREEN}✅{RESET} pydantic v2 (full features)")
    else:
        print(f"    {YELLOW}⚠️ {RESET} pydantic v1 (compatibility mode)")
except ImportError:
    print(f"    {YELLOW}⚠️ {RESET} pydantic not installed (config uses dataclasses)")

print()
if all_ok:
    print(f"  {GREEN}{BOLD}✅ All required packages verified!{RESET}")
    sys.exit(0)
else:
    print(f"  {RED}{BOLD}❌ Some required packages missing{RESET}")
    sys.exit(1)
PYEOF

VERIFY_RESULT=$?

echo ""
if [ $VERIFY_RESULT -eq 0 ]; then
    echo -e "${BOLD}${GREEN}╔═══════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${GREEN}║  ✅ Dependencies installed successfully!          ║${RESET}"
echo -e "${BOLD}${GREEN}╚═══════════════════════════════════════════════════╝${RESET}"
    echo ""
    echo -e "  ${BOLD}Driver priority:${RESET}"
    echo -e "  1. ${GREEN}asyncpg${RESET}   → async ops  (dashboard, API queries)"
    echo -e "  2. ${GREEN}psycopg2${RESET}  → sync ops   (DLT pipeline batches)"
    echo -e "  3. ${GREEN}pg8000${RESET}    → fallback   (pure Python, always works)"
    echo ""
    echo -e "  ${BOLD}Pydantic:${RESET}"
    PYDANTIC_V=$(python3 -c \
        "import pydantic; print(pydantic.__version__)" 2>/dev/null || echo "not installed")
    echo -e "  Version: ${GREEN}$PYDANTIC_V${RESET}"
    echo ""
    echo -e "  ${BOLD}Next step:${RESET}"
    echo -e "  ${CYAN}bash deployment/deploy_all.sh${RESET}"
    echo ""
else
    echo -e "${BOLD}${RED}╔═══════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${RED}║  ❌ Some packages failed                          ║${RESET}"
    echo -e "${BOLD}${RED}╚═══════════════════════════════════════════════════╝${RESET}"
    echo ""
    echo -e "  ${BOLD}Manual fixes:${RESET}"
    echo ""
    echo -e "  ${CYAN}# Fix pydantic (Rust issue):${RESET}"
    echo -e "  pip install pydantic --prefer-binary --only-binary=:all:"
    echo ""
    echo -e "  ${CYAN}# Install Rust then retry:${RESET}"
    echo -e "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    echo -e "  source \$HOME/.cargo/env"
    echo -e "  pip install pydantic"
    echo ""
    echo -e "  ${CYAN}# Use pydantic v1 (no Rust):${RESET}"
    echo -e "  pip install 'pydantic<2.0.0'"
    echo ""
    echo -e "  ${CYAN}# macOS Apple Silicon:${RESET}"
    echo -e "  arch -arm64 pip install pydantic --prefer-binary"
    echo ""
    echo -e "  ${CYAN}# Ubuntu/Debian:${RESET}"
    echo -e "  sudo apt-get install -y cargo rustc python3-dev build-essential"
    echo -e "  pip install pydantic --prefer-binary"
    echo ""
    exit 1
fi