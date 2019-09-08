#
# Remote State
terraform {
  backend "s3" {
    bucket      = "dellis-udacity-terraform"
    key         = "project2.tfstate"
    region      = "us-east-1"
    encrypt     = true
    kms_key_id  = "fbcc800a-8a44-405c-8ed8-20ab968fe1fa"
  }
}
