#!/usr/bin/env bash
# Whenever SSH to the cluster starts timing out (you moved networks / IP changed),
# run this from the repo root:  ./scripts/update-ssh-ip.sh
# It detects your current public IP, writes it into terraform.tfvars, then shows a
# Terraform plan and applies ONLY after you confirm. It never -auto-approve's an infra change.
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

# Show the plan and let you review it. We deliberately do NOT use -auto-approve:
# `terraform apply` re-plans the whole config, so an unreviewed apply could reconcile
# unrelated drift. Save the reviewed plan and apply exactly that.
PLAN_FILE="$(mktemp -t ssh-ip.XXXX.tfplan)"
trap 'rm -f "$PLAN_FILE"' EXIT

terraform plan -out="$PLAN_FILE"
echo
read -r -p "Apply the plan shown above? [y/N] " answer
if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
  terraform apply "$PLAN_FILE"
  echo "Done — SSH is now allowed from ${IP}/32"
else
  echo "Aborted. No changes applied (terraform.tfvars still shows ${IP}/32)."
fi
