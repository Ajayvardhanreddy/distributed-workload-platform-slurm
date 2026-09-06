#!/usr/bin/env bash
# Runs periodically on each compute node (via slurm.conf HealthCheckProgram), as root.
# If any check fails, it DRAINs its own node so the scheduler stops placing new work here.
set -euo pipefail

NODE="$(hostname -s)"
REASONS=()

# Test hook: lets us simulate a fault safely by creating this file.
if [[ -f /tmp/slurm-force-fail ]]; then
    REASONS+=("forced_failure")
fi

# Real check 1: root filesystem not near-full.
ROOT_USAGE="$(df -P / | awk 'NR==2 {gsub("%","",$5); print $5}')"
if (( ROOT_USAGE >= 90 )); then
    REASONS+=("root_disk_${ROOT_USAGE}pct")
fi

# Real check 2: /tmp is writable (jobs need scratch).
TEST_FILE="/tmp/slurm-healthcheck.$$"
if ! touch "$TEST_FILE" 2>/dev/null; then
    REASONS+=("tmp_not_writable")
else
    rm -f "$TEST_FILE"
fi

if (( ${#REASONS[@]} > 0 )); then
    REASON="$(IFS=,; echo "${REASONS[*]}")"
    logger -t slurm-healthcheck "draining ${NODE}: ${REASON}"
    /usr/bin/scontrol update NodeName="$NODE" State=DRAIN Reason="healthcheck:${REASON}"
    exit 1
fi

logger -t slurm-healthcheck "${NODE} healthy"
exit 0
