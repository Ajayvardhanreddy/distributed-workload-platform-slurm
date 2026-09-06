#!/bin/bash
#SBATCH --job-name=timing
#SBATCH --partition=eda
#SBATCH --time=00:02:00
#SBATCH --output=timing-%j.out

echo "Timing analysis on $(hostname) at $(date)"
sleep 15
echo "Timing complete"
