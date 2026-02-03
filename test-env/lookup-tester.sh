#!/bin/bash
# Lookup tester logs by name or IP hint
# Usage: ./lookup-tester.sh <tester_name_or_ip_hint>
#
# Examples:
#   ./lookup-tester.sh merijeek
#   ./lookup-tester.sh 158        # Last octet
#   ./lookup-tester.sh 71.184     # Partial IP

TESTERS_FILE="/mnt/ai_brain_ssd/projects/library-manager/.testers.json"
SKALDLEITA_LOG="/mnt/bookdb-ssd/bookdb/logs/api.log"
ID_QUEUE_DB="/mnt/bookdb-ssd/bookdb/src/identification_queue.db"
STAGING_DB="/mnt/bookdb-ssd/bookdb/data/staging.db"

if [ -z "$1" ]; then
    echo "Usage: $0 <tester_name_or_ip_hint>"
    echo ""
    echo "Known testers (from .testers.json):"
    if [ -f "$TESTERS_FILE" ]; then
        jq -r '.testers | keys[]' "$TESTERS_FILE" 2>/dev/null
    fi
    exit 1
fi

SEARCH="$1"

echo "=== Searching for: $SEARCH ==="
echo ""

# Check if it's a known tester name
if [ -f "$TESTERS_FILE" ]; then
    TESTER_INFO=$(jq -r ".testers.\"$SEARCH\" // empty" "$TESTERS_FILE" 2>/dev/null)
    if [ -n "$TESTER_INFO" ]; then
        echo "=== Tester Profile ==="
        echo "$TESTER_INFO" | jq .
        echo ""

        # Get IP hints for this tester
        IP_HINTS=$(echo "$TESTER_INFO" | jq -r '.ip_hints[]? // empty' 2>/dev/null)
        if [ -n "$IP_HINTS" ]; then
            SEARCH="$IP_HINTS"
            echo "Using IP hint: $SEARCH"
        fi
    fi
fi

echo "=== Recent Skaldleita Activity (last 50 matches) ==="
grep "$SEARCH" "$SKALDLEITA_LOG" 2>/dev/null | tail -50

echo ""
echo "=== Identification Jobs ==="
sqlite3 "$ID_QUEUE_DB" "SELECT ticket_id, user_id, status, folder_hint, datetime(created_at) as created FROM identification_jobs WHERE user_id LIKE '%$SEARCH%' ORDER BY created_at DESC LIMIT 20;" 2>/dev/null

echo ""
echo "=== Request Stats ==="
grep "$SEARCH" "$SKALDLEITA_LOG" 2>/dev/null | wc -l | xargs echo "Total log entries:"
grep "$SEARCH" "$SKALDLEITA_LOG" 2>/dev/null | grep "POST /api/identify_audio" | wc -l | xargs echo "Audio submissions:"
grep "$SEARCH" "$SKALDLEITA_LOG" 2>/dev/null | grep "POST /match" | wc -l | xargs echo "Text matches:"
