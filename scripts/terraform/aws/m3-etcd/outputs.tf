locals {
  this_ip = "${compact(concat(aws_instance.m3-etcd.*.private_ip, list("")))}"
}

output "name" {
  value = "${var.instance_name}"
}

output "ip" {
  value = "${local.this_ip}"
}
