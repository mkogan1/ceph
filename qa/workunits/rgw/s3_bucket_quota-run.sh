#!/bin/bash
set -e
cpanm --sudo Amazon::S3
exec perl rgw/s3_bucket_quota.pl
