#!/usr/bin/env bash
# Whenever SSH to the cluster starts timing out (you moved networks / IP changed),
# run this from the repo root:  ./scripts/update-ssh-ip.sh
# It detects your current public IP, writes it into terraform.tfvars, and updates
# ONLY the security-group SSH rule. No instances are touched. Costs ~nothing.
set -euo pipefail

cd "$(dirname "$0")/../infra/terraform"

IP="$(curl -s https://checkip.amazonaws.com)"
if [[ -z "$IP" ]]; then
  echo "Could not detect public IP (no network?)." >&2
  exit 1
fi
echo "Detected public IP: $IP"

# Replace the allowed_ssh_cidr line in terraform.tfvars
sed -i.bak -E "s|^allowed_ssh_cidr.*|allowed_ssh_cidr = \"${IP}/32\"|" terraform.tfvars
rm -f terraform.tfvars.bak
echo "Set allowed_ssh_cidr = \"${IP}/32\" in terraform.tfvars"

# Apply just the SG change. Drop -auto-approve if you'd rather review the plan first.
terraform apply -auto-approve

echo "Done — SSH is now allowed from ${IP}/32"
