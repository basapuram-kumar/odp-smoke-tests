#!/usr/bin/env bash
#
# Rebuild the interactive HTML dashboard from an existing JSON report.
#
# Usage:
#   ./view-log-verify-report.sh
#   ./view-log-verify-report.sh reports/log-verify/latest.json
#   ./view-log-verify-report.sh reports/log-verify/cluster-log-verify-YYYYMMDD-HHMMSS.json
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPORT_DIR="${SCRIPT_DIR}/reports/log-verify"
JSON_IN="${1:-${REPORT_DIR}/latest.json}"

die() { echo "[ERROR] $*" >&2; exit 1; }

[[ -f "$JSON_IN" ]] || die "JSON report not found: $JSON_IN (run cluster-service-log-verify.sh first)"

if [[ "$(basename "$JSON_IN")" == "latest.json" ]]; then
  OUT_HTML="${REPORT_DIR}/latest.html"
else
  OUT_HTML="${JSON_IN%.json}.html"
fi

export SCRIPT_DIR
export JSON_IN OUT_HTML
python3 - <<PY
import json
import os
import sys
from pathlib import Path

script_dir = os.environ["SCRIPT_DIR"]
sys.path.insert(0, os.path.join(script_dir, "lib"))
from cluster_log_verify import write_html_report

src = Path(os.environ["JSON_IN"])
dst = Path(os.environ["OUT_HTML"])
payload = json.loads(src.read_text(encoding="utf-8"))
write_html_report(payload, dst)
print(dst)
PY

echo "[INFO] HTML dashboard: ${OUT_HTML}"
echo "[INFO] Open in a browser, or serve it:"
echo "       cd $(dirname "$OUT_HTML") && python3 -m http.server 8765"
echo "       then browse http://<this-host>:8765/$(basename "$OUT_HTML")"
