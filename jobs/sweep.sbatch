#!/bin/bash
#SBATCH --job-name=eda-sweep
#SBATCH --partition=eda
#SBATCH --array=0-19%2          # 20 tasks (seeds 0..19), at most 2 running at once
#SBATCH --cpus-per-task=1
#SBATCH --mem=128M
#SBATCH --time=00:03:00
#SBATCH --output=sweep-%A_%a.out  # %A = array job id, %a = task id

SEED=$SLURM_ARRAY_TASK_ID

echo "Array job: $SLURM_ARRAY_JOB_ID"
echo "Task:      $SLURM_ARRAY_TASK_ID"
echo "Seed:      $SEED"
echo "Node:      $(hostname)"

python3 - "$SEED" <<'PY'
import random, sys, time
seed = int(sys.argv[1]); random.seed(seed)
result = sum(random.random() for _ in range(500_000))
print(f"seed={seed}, result={result:.4f}")
time.sleep(5)
PY
