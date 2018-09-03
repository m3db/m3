# m3-query

A terraform module to spin up an ASG for the m3-query service.

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|:----:|:-----:|:-----:|
| ami_id | Specify the exact AMI ID to use to build the instance. | string | - | yes |
| asg_des | The desired amount of nodes for the Auto Scale Group. | string | - | yes |
| asg_max | The maximum amount of nodes for the Auto Scale Group. | string | - | yes |
| asg_min | The minimum amount of nodes required for the Auto Scale Group. | string | - | yes |
| config_source | Provide a path or URL to the configuration source. Useful for auditing. | string | - | yes |
| instance_profile | IAM instance profile name to attach to the instance. | string | - | yes |
| instance_type | Specify the exact instance type to build. | string | - | yes |
| sg_ids | A list of security group IDs taht the ASG instances will belong to. | list | - | yes |
| ssh_key_name | ssh key pair name to attach to the instance. | string | - | yes |
| subnet_ids | A list of subnets IDs that the ASG instances will belong to. | list | - | yes |
| use_public_ip | Whether or not to attach a public IP to the nodes. | string | `true` | no |
| user_data_script | A script, generally a shell script, used to provision the instances. | string | - | yes |

## TODO

* Add in scaling policies. Right now the amount of query nodes should match asg_des, so scaling this up and down is mostly manual until a scaling policy is defined.
