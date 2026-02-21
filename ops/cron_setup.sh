#!/bin/bash
#
# Install / uninstall the daily procurement pipeline via macOS launchd.
#
# Usage:
#   ./cron_setup.sh install     # Prompts for API key, installs + loads plist
#   ./cron_setup.sh uninstall   # Unloads + removes plist
#

set -euo pipefail

PLIST_NAME="com.procurement.daily"
PLIST_SRC="$(cd "$(dirname "$0")" && pwd)/com.procurement.daily.plist"
PLIST_DEST="$HOME/Library/LaunchAgents/$PLIST_NAME.plist"
WORKING_DIR="$(cd "$(dirname "$0")" && pwd)"

install() {
    echo "=== Procurement Daily Pipeline — Install ==="

    # Prompt for API key
    if [ -z "${SAM_API_KEY:-}" ]; then
        read -rp "Enter your SAM.gov API key: " SAM_API_KEY
    fi
    if [ -z "$SAM_API_KEY" ]; then
        echo "Error: API key cannot be empty."
        exit 1
    fi

    # Create LaunchAgents dir if needed
    mkdir -p "$HOME/Library/LaunchAgents"

    # Copy plist and fill placeholders
    sed -e "s|__WORKING_DIR__|$WORKING_DIR|g" \
        -e "s|__SAM_API_KEY__|$SAM_API_KEY|g" \
        "$PLIST_SRC" > "$PLIST_DEST"

    # Load (unload first if already loaded)
    launchctl unload "$PLIST_DEST" 2>/dev/null || true
    launchctl load "$PLIST_DEST"

    echo ""
    echo "Installed: $PLIST_DEST"
    echo "Schedule:  Daily at 6:00 AM"
    echo "Logs:      /tmp/procurement_daily.log"
    echo ""
    echo "Verify with: launchctl list | grep procurement"
}

uninstall() {
    echo "=== Procurement Daily Pipeline — Uninstall ==="

    if [ -f "$PLIST_DEST" ]; then
        launchctl unload "$PLIST_DEST" 2>/dev/null || true
        rm "$PLIST_DEST"
        echo "Removed: $PLIST_DEST"
    else
        echo "Nothing to uninstall — plist not found."
    fi
}

case "${1:-}" in
    install)   install ;;
    uninstall) uninstall ;;
    *)
        echo "Usage: $0 {install|uninstall}"
        exit 1
        ;;
esac
