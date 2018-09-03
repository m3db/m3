resource "aws_autoscaling_group" "m3coord" {
  name                 = "m3-coordinator"
  launch_configuration = "${aws_launch_configuration.m3coord.id}"
  min_size             = "${var.asg_min}"
  max_size             = "${var.asg_max}"
  desired_capacity     = "${var.asg_des}"
  vpc_zone_identifier  = ["${var.subnet_ids}"]

  lifecycle {
    create_before_destroy = true
  }

  tag {
    key                 = "Application"
    value               = "m3"
    propagate_at_launch = "true"
  }

  tag {
    key                 = "configuration-source"
    value               = "${var.config_source}"
    propagate_at_launch = "true"
  }
}

resource "aws_elb" "m3coord" {
  name     = "m3-coordinator"
  subnets  = ["${var.subnet_ids}"]
  internal = true

  security_groups = ["${var.sg_ids}"]

  listener {
    instance_port     = 7201
    instance_protocol = "tcp"
    lb_port           = 7201
    lb_protocol       = "tcp"
  }

  cross_zone_load_balancing   = true
  idle_timeout                = 4000
  connection_draining         = true
  connection_draining_timeout = 60

  health_check {
    healthy_threshold   = 2
    unhealthy_threshold = 2
    target              = "HTTP:7203/metrics"
    interval            = 5
    timeout             = 2
  }
}

# attach ASG to ELB
resource "aws_autoscaling_attachment" "m3coord" {
  autoscaling_group_name = "m3-coordinator"
  elb                    = "${aws_elb.m3coord.id}"
}

# one launch config to rule them all.
resource "aws_launch_configuration" "m3coord" {
  name_prefix                 = "m3-coordinator"
  key_name                    = "${var.ssh_key_name}"
  image_id                    = "${var.ami_id}"
  instance_type               = "${var.instance_type}"
  security_groups             = ["${var.sg_ids}"]
  associate_public_ip_address = "${var.use_public_ip}"
  enable_monitoring           = true
  iam_instance_profile        = "${var.instance_profile}"

  root_block_device {
    volume_type           = "gp2"
    volume_size           = 60
    delete_on_termination = "true"
  }

  user_data = <<-EOF
                "${var.user_data_script}"
                EOF

  lifecycle {
    create_before_destroy = true
  }
}
