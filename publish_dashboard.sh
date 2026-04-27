#!/bin/bash
# Usage: ./publish_dashboard.sh
# Pushes grafana/dashboards/airflow_observability.json to a running Grafana instance.
# Does NOT require a Grafana restart.

# Get the absolute path of the directory where this script lives
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
GRAFANA_URL="${GRAFANA_URL:-http://admin:admin@localhost:3000}"
DASHBOARD_DIR="$BASE_DIR/grafana/dashboards"

# Ensure the directory exists
if [ ! -d "$DASHBOARD_DIR" ]; then
    echo "❌ Error: Dashboard directory not found at $DASHBOARD_DIR"
    exit 1
fi

# Enable nullglob so the loop doesn't run if no .json files exist
shopt -s nullglob

for DASHBOARD_FILE in "$DASHBOARD_DIR"/*.json; do
    echo "Publishing $(basename "$DASHBOARD_FILE") → $GRAFANA_URL ..."

    # Strip version/id so Grafana can manage its own internal DB state
    PAYLOAD=$(python3 -c "
import json
with open('$DASHBOARD_FILE') as f:
    d = json.load(f)
d.pop('version', None)
d.pop('id', None)
print(json.dumps({'dashboard': d, 'overwrite': True, 'folderId': 0}))
")

    RESULT=$(curl -s -X POST "$GRAFANA_URL/api/dashboards/db" \
      -H "Content-Type: application/json" \
      -d "$PAYLOAD")

    STATUS=$(echo "$RESULT" | python3 -c "import json,sys; r=json.load(sys.stdin); print(r.get('status','error'))")
    
    if [ "$STATUS" = "success" ]; then
      VERSION=$(echo "$RESULT" | python3 -c "import json,sys; r=json.load(sys.stdin); print(r.get('version','?'))")
      echo "   ✅ Success (v$VERSION)"
    else
      echo "   ❌ Failed: $RESULT"
    fi
done

echo "Done. View all dashboards at http://localhost:3000/dashboards"
