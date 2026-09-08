variable "aws_region" {
  description = "AWS region to build the lab in"
  type        = string
  default     = "us-east-1"
}

variable "allowed_ssh_cidr" {
  description = "Your public IP as a single-host CIDR, e.g. 1.2.3.4/32. Must be a /32; 0.0.0.0/0 is rejected."
  type        = string

  # Encode the security invariant in code, not just a comment: reject anything that
  # isn't a valid single-host (/32) CIDR — in particular a world-open 0.0.0.0/0.
  validation {
    condition = (
      can(cidrnetmask(var.allowed_ssh_cidr)) &&
      endswith(var.allowed_ssh_cidr, "/32") &&
      var.allowed_ssh_cidr != "0.0.0.0/0"
    )
    error_message = "allowed_ssh_cidr must be a valid single-host CIDR ending in /32 (e.g. 203.0.113.10/32) — never a broad range like 0.0.0.0/0."
  }
}

variable "public_key_path" {
  description = "Path to the SSH public key baked into the instances"
  type        = string
  default     = "~/.ssh/slurm-lab.pub"
}

variable "controller_instance_type" {
  description = "EC2 type for the controller (keep small for cost)"
  type        = string
  default     = "t3.micro"
}

variable "compute_instance_type" {
  description = "EC2 type for each compute node (keep small for cost)"
  type        = string
  default     = "t3.micro"
}
