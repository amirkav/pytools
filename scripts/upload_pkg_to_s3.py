#!/usr/bin/env python

import sys
import boto3


def deploy(env: str, version: str) -> None:
    bucket = f"amir-personal-deployment"
    s3_client = boto3.client("s3")
    pkg_name = "pytools"

    s3_keys = [f"{pkg_name}.zip", f"{pkg_name}-{version}.zip"]

    for key in s3_keys:
        print(f"Copying latest {pkg_name} package to s3://{bucket}/{pkg_name}/{key}")
        s3_client.upload_file(
            Bucket=bucket,
            Key=f"{pkg_name}/{key}",
            Filename=f"{pkg_name}-{version}.zip",
        )


if __name__ == "__main__":
    deploy(env=sys.argv[1], version=sys.argv[2])
