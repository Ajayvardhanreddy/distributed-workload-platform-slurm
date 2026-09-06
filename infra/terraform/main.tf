# ---------------------------------------------------------------------------
# AMI lookup: latest Ubuntu 24.04 LTS (noble), x86_64, from Canonical's
# published SSM parameter. Using this instead of a hardcoded ami-xxxx means we
# always get the current patched image and it works in any region.
# ---------------------------------------------------------------------------
data "aws_ssm_parameter" "ubuntu_2404" {
  name = "/aws/service/canonical/ubuntu/server/noble/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

# ---------------------------------------------------------------------------
# Network: one VPC -> one public subnet -> internet gateway -> route table.
# This is the private network the cluster lives on (10.20.0.0/16).
# ---------------------------------------------------------------------------
resource "aws_vpc" "slurm" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true # lets instances resolve DNS at all
  enable_dns_hostnames = true # gives instances DNS names

  tags = { Name = "slurm-eda-vpc" }
}

# The door between our VPC and the public internet (for apt / package downloads
# and for us to SSH in). Without it, the subnet is fully isolated.
resource "aws_internet_gateway" "slurm" {
  vpc_id = aws_vpc.slurm.id
  tags   = { Name = "slurm-eda-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.slurm.id
  cidr_block              = "10.20.1.0/24"
  map_public_ip_on_launch = true # instances get a public IP automatically
  availability_zone       = "${var.aws_region}a"

  tags = { Name = "slurm-eda-public" }
}

# A route table is the subnet's "how do I reach X" map. This one says:
# everything not inside the VPC (0.0.0.0/0) goes out through the internet gateway.
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.slurm.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.slurm.id
  }

  tags = { Name = "slurm-eda-public-rt" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# ---------------------------------------------------------------------------
# Security group = stateful virtual firewall around the instances.
# ---------------------------------------------------------------------------
resource "aws_security_group" "slurm" {
  name        = "slurm-eda-sg"
  description = "Security group for SLURM learning cluster"
  vpc_id      = aws_vpc.slurm.id

  tags = { Name = "slurm-eda-sg" }
}

# SSH only from YOUR IP. This is the single rule standing between the internet
# and port 22 on your nodes — hence allowed_ssh_cidr has no default.
resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.slurm.id
  description       = "SSH only from my public IP"

  cidr_ipv4   = var.allowed_ssh_cidr
  from_port   = 22
  to_port     = 22
  ip_protocol = "tcp"
}

# Allow ALL traffic between members of this same security group. SLURM/MUNGE use
# several ports; rather than enumerate them, we trust intra-cluster traffic.
# Note: this references the SG itself, NOT a CIDR — so it only covers our nodes.
resource "aws_vpc_security_group_ingress_rule" "cluster_internal" {
  security_group_id = aws_security_group.slurm.id
  description       = "Allow all traffic between cluster members"

  referenced_security_group_id = aws_security_group.slurm.id
  ip_protocol                  = "-1" # -1 = all protocols/ports
}

# Outbound: allow everything (needed for apt updates, SSM, etc.).
resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.slurm.id
  description       = "Allow outbound package downloads and updates"

  cidr_ipv4   = "0.0.0.0/0"
  ip_protocol = "-1"
}

# ---------------------------------------------------------------------------
# Key pair: uploads our PUBLIC key so we can SSH in as 'ubuntu' with no password.
# ---------------------------------------------------------------------------
resource "aws_key_pair" "slurm" {
  key_name   = "slurm-eda-lab"
  public_key = file(pathexpand(var.public_key_path))
}

# ---------------------------------------------------------------------------
# Instances. Static private IPs so /etc/hosts is stable and predictable.
# user_data runs once at first boot to set the hostname and populate /etc/hosts
# so nodes can resolve each other by short name (controller/compute-01/02).
# ---------------------------------------------------------------------------
resource "aws_instance" "controller" {
  ami                         = data.aws_ssm_parameter.ubuntu_2404.value
  instance_type               = var.controller_instance_type
  subnet_id                   = aws_subnet.public.id
  private_ip                  = "10.20.1.10"
  associate_public_ip_address = true
  key_name                    = aws_key_pair.slurm.key_name
  vpc_security_group_ids      = [aws_security_group.slurm.id]

  metadata_options {
    http_tokens = "required" # enforce IMDSv2 (blocks a class of SSRF creds theft)
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 8
    encrypted   = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -eux
    hostnamectl set-hostname controller
    cat >> /etc/hosts <<'HOSTS'
    10.20.1.10 controller
    10.20.1.11 compute-01
    10.20.1.12 compute-02
    HOSTS
  EOF

  tags = {
    Name = "slurm-controller"
    Role = "controller"
  }
}

resource "aws_instance" "compute" {
  for_each = {
    "compute-01" = "10.20.1.11"
    "compute-02" = "10.20.1.12"
  }

  ami                         = data.aws_ssm_parameter.ubuntu_2404.value
  instance_type               = var.compute_instance_type
  subnet_id                   = aws_subnet.public.id
  private_ip                  = each.value
  associate_public_ip_address = true
  key_name                    = aws_key_pair.slurm.key_name
  vpc_security_group_ids      = [aws_security_group.slurm.id]

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 8
    encrypted   = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -eux
    hostnamectl set-hostname ${each.key}
    cat >> /etc/hosts <<'HOSTS'
    10.20.1.10 controller
    10.20.1.11 compute-01
    10.20.1.12 compute-02
    HOSTS
  EOF

  tags = {
    Name = each.key
    Role = "compute"
  }
}
