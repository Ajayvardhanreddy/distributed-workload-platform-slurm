#!/bin/bash
#SBATCH --job-name=hello-eda
#SBATCH --partition=eda
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=128M
#SBATCH --time=00:02:00
#SBATCH --output=hello-%j.out

echo "Job ID: $SLURM_JOB_ID"
echo "Node:   $(hostname)"
echo "CPUs:   $SLURM_CPUS_PER_TASK"
echo "Started:  $(date)"

sleep 10

echo "Finished: $(date)"
