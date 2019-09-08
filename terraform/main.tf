provider "aws" {
  profile = "udacity"
  region  = "us-east-1"
}

resource "aws_iam_role" "redshift_role" {
  name = "SparkifyReshiftRole"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "redshift_s3_policy" {
  name = "RedshiftS3Policy"
  role = "${aws_iam_role.redshift_role.id}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Effect": "Allow",
          "Action": [
              "s3:Get*",
              "s3:List*"
          ],
          "Resource": "*"
      }
  ]
}
EOF
}

data "aws_kms_secrets" "redshift_secrets" {
  secret {
    name    = "master_password"
    payload = "AQICAHi1LKNeuIjkusvwaHCrs67uvdcKIfeMYeoSHWv13xkW2wEisnfUjYfOqcsNMWdEAPBYAAAAaDBmBgkqhkiG9w0BBwagWTBXAgEAMFIGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQM4yjmmiLVOdZbe/VWAgEQgCUAuwAdA9ErHP7hpaXM9mcrz/RF4WnL9et49h6Zlor3z7REV6tg"
  }
}

resource "aws_redshift_cluster" "sparkify" {
  cluster_identifier  = "udacity-dwh-project"
  database_name       = "dev"
  master_username     = "awsuser"
  master_password     = "${data.aws_kms_secrets.redshift_secrets.plaintext["master_password"]}"
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  skip_final_snapshot = "true" #TODO: specify s3 resource for final snapshots
  iam_roles           = ["${aws_iam_role.redshift_role.arn}"]
}