#!/bin/bash
#SBATCH --job-name=synth
#SBATCH --partition=eda
#SBATCH --time=00:02:00
#SBATCH --output=synth-%j.out

echo "Synthesis on $(hostname) at $(date)"
sleep 15
echo "Synthesis complete"
