import logging
import psycopg2

# Class for managing a connection to a PosgreSQL database
class DbConnection(object):

    # Default constructor of the class. Uses default parameters if not provided.
    def __init__(self, parameters):
        self.cursor = None
        try:
            logging.info("Trying to connect to Redshift db: " + parameters["db_host"])
            connection = psycopg2.connect("dbname='" + parameters["db_name"] + "' user='" + parameters["db_user"] + "' host='" + parameters["db_host"] + "' password='" + parameters["db_password"] + "' port='" + parameters["db_port"] + "'")
            connection.autocommit = True
            # Create connection    
            self.cursor = connection.cursor()
        except psycopg2.Error as error:
            logging.error("I am unable to connect to the database - " + "dbname='" + parameters["db_name"] + "' user='" + parameters["db_user"] + "' host='" + parameters["db_host"] + "' port='" + parameters["db_port"] + "'")
            logging.error(error.pgerror)
            logging.error(error.diag.message_detail)
        except:
            logging.error("Something went wrong initializing S3Manager")
            sys.exit()