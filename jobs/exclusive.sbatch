#!/bin/bash
#SBATCH --job-name=exclusive
#SBATCH --partition=eda
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --time=00:05:00
#SBATCH --output=exclusive-%j.out

# --exclusive = this job takes a WHOLE node, even if it only needs 1 CPU.
echo "Running job $SLURM_JOB_ID on $(hostname)"
sleep 180
