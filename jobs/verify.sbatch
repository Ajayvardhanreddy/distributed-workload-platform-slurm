#!/bin/bash
#SBATCH --job-name=verify
#SBATCH --partition=eda
#SBATCH --time=00:02:00
#SBATCH --output=verify-%j.out

echo "Verification on $(hostname) at $(date)"
sleep 15
echo "Verification complete"
