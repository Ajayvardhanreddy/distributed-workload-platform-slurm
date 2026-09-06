#!/bin/bash
#SBATCH --job-name=synth-fail
#SBATCH --partition=eda
#SBATCH --time=00:02:00
#SBATCH --output=synth-fail-%j.out

echo "Synthesis on $(hostname) - will FAIL"
sleep 5
echo "Synthesis FAILED"
exit 1
