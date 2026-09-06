variable "aws_region" {
  description = "AWS region to build the lab in"
  type        = string
  default     = "us-east-1"
}

variable "allowed_ssh_cidr" {
  description = "Your public IP in CIDR form, e.g. 1.2.3.4/32. NEVER set this to 0.0.0.0/0."
  type        = string
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
