import logging
from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.key import Key
import defaults

#Class for managing s3 buckets

class S3Manager(object):

    # Default constructor of the class. Uses default parameters if not provided.
    def __init__(self, parameters={}):
        self.access_key = parameters["access_key"] if "access_key" in parameters else defaults.access_key
        self.secret_key = parameters["secret_key"] if "secret_key" in parameters else defaults.secret_key
        self.connection = S3Connection(self.access_key, self.secret_key)

    # Create a bucket in the default region
    def create_bucket(self, bucket_name, location = Location.EU):
        return self.connection.create_bucket(bucket_name, location=location)

    # Delete a bucket in the default region
    def delete_bucket(self, bucket_name):
        self.connection.delete_bucket(bucket_name)

    # Upload a file to a bucket
    def upload_file(self, bucket, local_file_name, remote_file_name):
        theKey = Key(bucket)
        theKey.key = remote_file_name
        theKey.set_contents_from_filename(local_file_name)
