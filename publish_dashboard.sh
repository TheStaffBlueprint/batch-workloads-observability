#!/bin/bash
# Usage: ./publish_dashboard.sh
# Pushes grafana/dashboards/airflow_observability.json to a running Grafana instance.
# Does NOT require a Grafana restart.

GRAFANA_URL="${GRAFANA_URL:-http://admin:admin@localhost:3000}"
DASHBOARD_FILE="grafana/dashboards/airflow_observability.json"

echo "Publishing $DASHBOARD_FILE → $GRAFANA_URL ..."

# Strip the `version` field before pushing — Grafana manages its own internal
# version counter and will reject the push if the number doesn't match.
# The `version` in the local file is used only by the file-provisioner poller.
PAYLOAD=$(python3 -c "
import json, sys

with open('$DASHBOARD_FILE') as f:
    d = json.load(f)

d.pop('version', None)   # let Grafana own the version counter
d.pop('id', None)        # let Grafana own the internal DB id

print(json.dumps({'dashboard': d, 'overwrite': True, 'folderId': 0}))
")

RESULT=$(curl -s -X POST "$GRAFANA_URL/api/dashboards/db" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

STATUS=$(echo "$RESULT" | python3 -c "import json,sys; r=json.load(sys.stdin); print(r.get('status','error'))")
URL=$(echo "$RESULT" | python3 -c "import json,sys; r=json.load(sys.stdin); print(r.get('url',''))")
VERSION=$(echo "$RESULT" | python3 -c "import json,sys; r=json.load(sys.stdin); print(r.get('version','?'))")

if [ "$STATUS" = "success" ]; then
  echo "✅ Dashboard published (Grafana internal version: $VERSION)"
  echo "   Open: http://localhost:3000$URL"
else
  echo "❌ Failed: $RESULT"
  exit 1
fi
