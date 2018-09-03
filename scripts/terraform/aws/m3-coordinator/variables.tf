variable "ami_id" {
  description = "Specify the exact AMI ID to use to build the instance."
}

variable "instance_type" {
  description = "Specify the exact instance type to build."
}

variable "ssh_key_name" {
  description = "ssh key pair name to attach to the instance."
}

variable "instance_profile" {
  description = "IAM instance profile name to attach to the instance."
}

variable "config_source" {
  description = "Provide a path or URL to the configuration source. Useful for auditing."
}

variable "asg_min" {
  description = "The minimum amount of nodes required for the Auto Scale Group."
}

variable "asg_des" {
  description = "The desired amount of nodes for the Auto Scale Group."
}

variable "asg_max" {
  description = "The maximum amount of nodes for the Auto Scale Group."
}

variable "use_public_ip" {
  default     = true
  description = "Whether or not to attach a public IP to the nodes."
}

variable "user_data_script" {
  description = "A script, generally a shell script, used to provision the instances."
}

variable "subnet_ids" {
  type        = "list"
  description = "A list of subnets IDs that the ASG instances will belong to."
}

variable "sg_ids" {
  type        = "list"
  description = "A list of security group IDs taht the ASG instances will belong to."
}
