# durable network interfaces
resource "aws_network_interface" "m3-etcd" {
  subnet_id       = "${var.subnet_id}"
  security_groups = ["${var.sg_ids}"]

  tags {
    Application          = "m3"
    Name                 = "${var.instance_name}"
    configuration-source = "${var.config_source}"
  }
}

# public elastic IPs
resource "aws_eip" "m3-etcd" {
  vpc               = true
  network_interface = "${aws_network_interface.m3-etcd.id}"

  tags {
    Application          = "m3"
    Name                 = "${var.instance_name}"
    configuration-source = "${var.config_source}"
  }
}

# associate elastic ip with network interface
resource "aws_eip_association" "m3-etcd" {
  allocation_id        = "${aws_eip.m3-etcd.id}"
  network_interface_id = "${aws_network_interface.m3-etcd.id}"
}

resource "aws_instance" "m3-etcd" {
  ami           = "${var.ami_id}"
  instance_type = "${var.instance_type}"
  key_name      = "${var.ssh_key_name}"

  monitoring                           = true
  instance_initiated_shutdown_behavior = "stop"
  disable_api_termination              = true
  ebs_optimized                        = true
  iam_instance_profile                 = "${var.instance_profile}"

  root_block_device {
    volume_type           = "gp2"
    volume_size           = "100"
    delete_on_termination = "true"
  }

  network_interface {
    device_index          = 0
    network_interface_id  = "${aws_network_interface.m3-etcd.id}"
    delete_on_termination = false
  }

  user_data = <<-EOF
                "${var.user_data_script}"
                EOF

  lifecycle {
    ignore_changes = ["user_data", "ami"]
  }

  tags {
    Name                 = "${var.instance_name}"
    Application          = "m3"
    configuration-source = "${var.config_source}"
  }
}
