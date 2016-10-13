import boto3
import sys

s3Bucket = sys.argv[1]
s3Key = sys.argv[2]
outfile = sys.argv[3]

s3 = boto3.resource('s3')
s3.meta.client.download_file(s3Bucket, s3Key, outfile)

