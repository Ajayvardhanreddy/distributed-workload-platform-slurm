output "controller_public_ip" {
  description = "Public IP of the controller (SSH here)"
  value       = aws_instance.controller.public_ip
}

output "controller_private_ip" {
  description = "Private IP of the controller (used inside the cluster)"
  value       = aws_instance.controller.private_ip
}

output "compute_public_ips" {
  description = "Public IPs of the compute nodes"
  value = {
    for name, instance in aws_instance.compute :
    name => instance.public_ip
  }
}

output "ssh_commands" {
  description = "Copy-paste SSH commands for each node"
  value = {
    controller = "ssh -i ~/.ssh/slurm-lab ubuntu@${aws_instance.controller.public_ip}"
    compute_01 = "ssh -i ~/.ssh/slurm-lab ubuntu@${aws_instance.compute["compute-01"].public_ip}"
    compute_02 = "ssh -i ~/.ssh/slurm-lab ubuntu@${aws_instance.compute["compute-02"].public_ip}"
  }
}
