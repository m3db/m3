# m3-etcd

A terraform module to spin up a single m3-etcd instance. Normally we would define a count variable and let you specify an arbitrary amount of instances to spin up, but as we've scaled up and our terraform usage has increased we've noticed oddities when doing that for larger count sizes.

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|:----:|:-----:|:-----:|
| ami_id | Specify the exact AMI ID to use to build the instance. | string | - | yes |
| config_source | Provide a path or URL to the configuration source. Useful for auditing. | string | - | yes |
| instance_name | The name of this instance. This will be assigned to the Name tag for the EC2 instance. | string | - | yes |
| instance_profile | IAM instance profile name to attach to the instance. | string | - | yes |
| instance_type | Specify the exact instance type to build. | string | - | yes |
| sg_ids | A list of security group IDs taht the ASG instances will belong to. | list | - | yes |
| ssh_key_name | ssh key pair name to attach to the instance. | string | - | yes |
| subnet_id | A single subnet ID that the DB instance will belong to. | string | - | yes |
| user_data_script | A script, generally a shell script, used to provision the instances. | string | - | yes |

## Outputs

| Name | Description |
|------|-------------|
| ip |  |
| name |  |
