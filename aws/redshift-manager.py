import logging
from boto.redshift.layer1 import RedshiftConnection
from boto.redshift import connect_to_region

# Class for managing Redshift clusters

class RedshiftManager(object):

    # Default constructor of the class.
    def __init__(self, parameters):
        try:
            self.access_key = parameters["access_key"]
            self.secret_key = parameters["secret_key"]
        except:
            logging.error("Something went wrong initializing RedshiftManager")
            sys.exit()

        # Create connection
        self.connection = RedshiftConnection(aws_access_key_id = self.access_key, aws_secret_access_key = self.secret_key)

    # Create clusters

    # Delete cluster