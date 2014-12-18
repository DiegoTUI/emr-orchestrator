import logging
import sys
import defaults
from boto.emr.step import StreamingStep
from aws.s3_manager import S3Manager

# Logger level
logging.getLogger().setLevel(logging.INFO)

# script to trigger the whole process

# create s3 bucket
def create_s3_bucket():
    pass

# upload files to s3 bucket

# upload mapper.py script to s3 bucket

# start EMR cluster

# run mapreduce in EMR cluster

# terminate EMR cluster

# copy contents of output to Redshift

# delete bucket

