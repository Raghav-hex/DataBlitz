#!/usr/bin/env bash
# =============================================================
# DataBlitz — Full Deployment Script
# Run this once on your local machine to go live.
# Prerequisites: node 18+, npm, python3, git
# =============================================================
set -euo pipefail

ACCOUNT_ID="f5f50e39cd78518e8c3fcd7fa90b96ea"
KV_NS_ID="517788ccef6d4a15af8267f0d926950a"
WORKER_NAME="datablitz-api"
REPO="https://github.com/Raghav-hex/DataBlitz"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║            DataBlitz — Deployment Script                 ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Account ID : $ACCOUNT_ID"
echo "KV NS ID   : $KV_NS_ID  (already created)"
echo "Worker     : $WORKER_NAME"
echo ""

# ── Step 1: Install wrangler ────────────────────────────────────────────────
echo "Step 1/5 — Installing wrangler..."
npm install -g wrangler@latest --silent
echo "  ✓ wrangler $(wrangler --version)"

# ── Step 2: Login to Cloudflare ────────────────────────────────────────────
echo ""
echo "Step 2/5 — Logging in to Cloudflare..."
echo "  (Browser will open — log in with Vraghav.vasudevan@gmail.com)"
wrangler login

# ── Step 3: Deploy the Worker ──────────────────────────────────────────────
echo ""
echo "Step 3/5 — Deploying Cloudflare Worker (datablitz-api)..."
wrangler deploy --name "$WORKER_NAME"
WORKER_URL="https://${WORKER_NAME}.$(wrangler whoami 2>/dev/null | grep 'subdomain' | awk '{print $2}' || echo 'YOUR-SUBDOMAIN').workers.dev"
echo "  ✓ Worker deployed"
echo "  → Test: curl https://${WORKER_NAME}.workers.dev/api/meta"

# ── Step 4: Set GitHub Secrets ─────────────────────────────────────────────
echo ""
echo "Step 4/5 — Setting GitHub Actions secrets..."

# Check for gh CLI
if command -v gh &>/dev/null; then
    echo "  Using GitHub CLI..."
    REPO_PATH="Raghav-hex/DataBlitz"

    gh secret set CF_ACCOUNT_ID     --body "$ACCOUNT_ID"      --repo "$REPO_PATH"
    gh secret set CF_KV_NAMESPACE_ID --body "$KV_NS_ID"       --repo "$REPO_PATH"
    gh secret set FRED_API_KEY       --body "614f4eefe41cec35a8ac23c93a9d500f" --repo "$REPO_PATH"
    gh secret set NOAA_CDO_TOKEN     --body "fFWcUkJSnXNaJcIzuincRgBoFnHBiMQl" --repo "$REPO_PATH"

    echo ""
    echo "  Still needed (paste manually):"
    echo "  → CF_API_TOKEN       (from dash.cloudflare.com/profile/api-tokens)"
    echo "  → PUTER_AUTH_TOKEN   (your JWT — see memory)"
    echo "  → GEMINI_API_KEY     (from aistudio.google.com)"

    echo ""
    read -p "  Paste CF_API_TOKEN now (or press Enter to skip): " cf_token
    if [ -n "$cf_token" ]; then
        gh secret set CF_API_TOKEN --body "$cf_token" --repo "$REPO_PATH"
        echo "  ✓ CF_API_TOKEN set"
    fi

    read -p "  Paste PUTER_AUTH_TOKEN now (or press Enter to skip): " puter_token
    if [ -n "$puter_token" ]; then
        gh secret set PUTER_AUTH_TOKEN --body "$puter_token" --repo "$REPO_PATH"
        echo "  ✓ PUTER_AUTH_TOKEN set"
    fi

    read -p "  Paste GEMINI_API_KEY now (or press Enter to skip): " gemini_key
    if [ -n "$gemini_key" ]; then
        gh secret set GEMINI_API_KEY --body "$gemini_key" --repo "$REPO_PATH"
        echo "  ✓ GEMINI_API_KEY set"
    fi
else
    echo "  gh CLI not found — set secrets manually at:"
    echo "  https://github.com/Raghav-hex/DataBlitz/settings/secrets/actions"
    echo ""
    echo "  Secrets to add:"
    echo "  CF_ACCOUNT_ID       = $ACCOUNT_ID"
    echo "  CF_KV_NAMESPACE_ID  = $KV_NS_ID"
    echo "  CF_API_TOKEN        = (from dash.cloudflare.com/profile/api-tokens)"
    echo "  FRED_API_KEY        = 614f4eefe41cec35a8ac23c93a9d500f"
    echo "  NOAA_CDO_TOKEN      = fFWcUkJSnXNaJcIzuincRgBoFnHBiMQl"
    echo "  PUTER_AUTH_TOKEN    = (your JWT token)"
    echo "  GEMINI_API_KEY      = (from aistudio.google.com)"
fi

# ── Step 5: Deploy Cloudflare Pages ───────────────────────────────────────
echo ""
echo "Step 5/5 — Cloudflare Pages (frontend)..."
echo "  This step requires browser — do it at:"
echo "  1. Go to: https://dash.cloudflare.com → Workers & Pages → Create → Pages"
echo "  2. Connect to Git → GitHub → Raghav-hex/DataBlitz"
echo "  3. Build command:      (leave empty)"
echo "  4. Output directory:   frontend"
echo "  5. Click Deploy"
echo ""
echo "  Your Pages URL will be something like: https://datablitz.pages.dev"
echo ""
echo "  !! After Pages is live, update frontend/index.html line:"
echo "     const API_BASE = 'https://datablitz-api.workers.dev';"
echo "  Then: git add frontend/index.html && git commit -m 'config: set production API_BASE' && git push"

# ── Done ───────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║              Deployment Summary                          ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  KV Namespace : CREATED ✓                               ║"
echo "║  Worker       : datablitz-api.workers.dev               ║"
echo "║  Pages        : Connect at dash.cloudflare.com (Step 5) ║"
echo "║  Pipeline     : Run at GitHub Actions → Run workflow     ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "After Pages is live, trigger your first run:"
echo "  https://github.com/Raghav-hex/DataBlitz/actions"
echo "  → DataBlitz Weekly Pipeline → Run workflow"
echo ""
