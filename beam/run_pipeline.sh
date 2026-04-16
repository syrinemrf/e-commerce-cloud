#!/usr/bin/env bash
# beam/run_pipeline.sh
# Purpose : Run the Apache Beam pipeline locally using DirectRunner.
# Author  : ProjetCloud Team
# Date    : 2024-06-01
#
# COST NOTE: DirectRunner runs on your local machine — $0 GCP compute cost.
# Only BigQuery WRITE_APPEND incurs a tiny batch load (always free).
#
# Usage:
#   bash beam/run_pipeline.sh
#   bash beam/run_pipeline.sh --limit 50

set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
LIMIT="${1:---limit=100}"

echo "[INFO] Running Beam pipeline with DirectRunner (local — \$0 GCP cost)"
echo "[INFO] Limit: ${LIMIT}"
echo ""

python "${SCRIPT_DIR}/pipeline.py" ${LIMIT}
