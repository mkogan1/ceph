#!/bin/bash
set -e
cpanm --sudo Amazon::S3
exec perl rgw/s3_multipart_upload.pl
