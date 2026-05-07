variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "i4i.4xlarge"
}

variable "ami_id" {
  description = "Custom AMI ID (leave empty for latest Ubuntu 22.04)"
  type        = string
  default     = ""
}

variable "root_volume_size" {
  description = "Root EBS volume size in GB"
  type        = number
  default     = 50
}

variable "environment" {
  description = "Environment name. Selects which crypto/<env>/us-east-1 snapshot bucket policies to attach to the EC2 instance role."
  type        = string
  default     = "dev"
}
