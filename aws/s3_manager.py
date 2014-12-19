import logging
from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.key import Key

#Class for managing s3 buckets

class S3Manager(object):

    # Default constructor of the class. Uses default parameters if not provided.
    def __init__(self, parameters):
        try:
            self.access_key = parameters["access_key"]
            self.secret_key = parameters["secret_key"]
        except:
            logging.error("Something went wrong initializing S3Manager")
            sys.exit()

        # Create connection    
        self.connection = S3Connection(self.access_key, self.secret_key)

    # Create a bucket in the default region
    def create_bucket(self, bucket_name, location = Location.EU):
        return self.connection.create_bucket(bucket_name, location=location)

    # Delete a bucket in the default region. It has to be empty before it's deleted.
    def delete_bucket(self, bucket_name):
        self.connection.delete_bucket(bucket_name)

    # Returns a bucket by name
    def get_bucket(self, bucket_name):
        return self.connection.get_bucket(bucket_name)

    # Upload a file to a bucket
    def upload_file(self, bucket_name, local_file_name, remote_file_name, callback=None):
        theKey = Key(self.get_bucket(bucket_name))
        theKey.key = remote_file_name
        theKey.set_contents_from_filename(local_file_name, cb=callback, num_cb=1)

    # Deletes all files with prefix
    def delete_files_with_prefix(self, bucket_name, prefix):
        bucket = self.get_bucket(bucket_name)
        keys_to_delete = bucket.list(prefix = prefix)
        bucket.delete_keys([key.name for key in keys_to_delete])

    # Empties the contents of a bucket
    def empty_bucket(self, bucket_name):
        bucket = self.get_bucket(bucket_name)
        keys_to_delete = bucket.get_all_keys()
        bucket.delete_keys([key.name for key in keys_to_delete])
