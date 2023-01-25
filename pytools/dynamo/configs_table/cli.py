#!/usr/bin/env python
"""
Main CLI entrypoint for `config_backup`

Usage:

```bash
config_manager [-h] [--region REGION] [--env ENV] [--restore]
                             [--bucket BUCKET] [-d]
                             PROJECT [PROJECT ...]

Backup and restore for DynamoDB data.

positional arguments:
  PROJECT

optional arguments:
  -h, --help       show this help message and exit
  --region REGION  AWS region.
  --env ENV        AWS environment.
  --restore        Restore table data from backup.
  --bucket BUCKET  Bucket name in S3.
  -d, --debug      Increases log verbosity.
```

"""
import argparse
import sys

from tools.dynamo.config_manager import ConfigsInterface, ConfigsInterfaceError
from tools.logger import Logger


def get_parser() -> argparse.ArgumentParser:
    """
    Create `argparse.ArgumentParser` instance for CLI usage.

    Returns:
        An `argparse.ArgumentParser` instance.
    """
    parser = argparse.ArgumentParser(
        "config_manager", description="Backup and restore for DynamoDB data."
    )
    parser.add_argument("project_names", metavar="PROJECT", nargs="+")
    parser.add_argument("--region", default="us-west-2", help="AWS region.")
    parser.add_argument("--env", default="dev", help="AWS environment.")
    parser.add_argument("--restore", action="store_true", help="Restore table data from backup.")
    parser.add_argument("--bucket", default="test-dynamo-backup-2", help="Bucket name in S3.")
    parser.add_argument("-d", "--debug", action="store_true", help="Increases log verbosity.")
    return parser


def main() -> None:
    """
    Main CLI entrypoint for `config_backup`
    """
    parser = get_parser()
    args = parser.parse_args()
    log_level = Logger.DEBUG if args.debug else Logger.INFO
    logger = Logger(__name__, level=log_level)
    logger.info(f"Starting manager for {args.region} region, {args.env} env")

    for project_name in args.project_names:
        config_manager = ConfigsInterface(project_id=project_name, s3_bucket=args.bucket)
        try:
            if args.restore:
                config_manager.reset()
            else:
                config_manager.backup()
        except ConfigsInterfaceError as e:
            logger.error(e)  # type: ignore
            sys.exit(1)


if __name__ == "__main__":
    main()
