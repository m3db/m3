variable "ami_id" {
  description = "Specify the exact AMI ID to use to build the instance."
}

variable "subnet_id" {
  description = "A single subnet ID that the DB instance will belong to."
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

variable "instance_name" {
  description = "The name of this instance. This will be assigned to the Name tag for the EC2 instance."
}

variable "user_data_script" {
  description = "A script, generally a shell script, used to provision the instances."
}

variable "sg_ids" {
  type        = "list"
  description = "A list of security group IDs taht the ASG instances will belong to."
}
