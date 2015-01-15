#!/usr/bin/env python

import logging
import sys
import os
import time
import defaults
from boto.emr.step import StreamingStep
from aws.s3_manager import S3Manager
from aws.emr_manager import EmrManager
from redshift.db_connection import DbConnection
from psycopg2 import Error

# Logger level
logging.getLogger().setLevel(logging.INFO)

# script to trigger the whole process

# globals
# S3
s3_parameters = {
    "access_key": defaults.access_key,
    "secret_key": defaults.secret_key
}
s3_manager = S3Manager(s3_parameters)
# Elastic Mapreduce
emr_parameters = {
    "region_name": defaults.region_name,
    "access_key": defaults.access_key,
    "secret_key": defaults.secret_key,
    "ec2_keypair_name": defaults.ec2_keypair_name,
    "base_bucket": "s3://" + defaults.bucket_name,
    "log_dir": defaults.log_dir,
    "emr_status_wait": defaults.emr_status_wait,
    "step_status_wait": defaults.step_status_wait,
    "emr_cluster_name": defaults.emr_cluster_name
}
emr_manager = EmrManager(emr_parameters)
# Redshift
redshift_parameters = {
    "db_name": defaults.db_name,
    "db_user": defaults.db_user,
    "db_password": defaults.db_password,
    "db_port": defaults.db_port,
    "db_host": defaults.db_host
}
redshift_cursor = DbConnection(redshift_parameters).cursor
# Mapper
mapper_uploaded = False
# Copy to local
copy_to_local_uploaded = False
# Jar uploaded
jar_uploaded = False

# create s3 bucket
def create_s3_bucket():
    logging.info("Creating bucket: " + defaults.bucket_name)
    s3_manager.create_bucket(defaults.bucket_name)
    logging.info("Bucket: " + defaults.bucket_name + " created.")


# upload log files to s3 bucket
def upload_files_to_s3_bucket():
    # Method not available. Let's consider using s3-parallel-put for this
    pass

# upload mapper.py script to s3 bucket
def upload_mapper_to_s3_bucket():
    global mapper_uploaded
    logging.info("Uploading mapper script")
    current_folder = os.path.dirname(os.path.realpath(__file__))
    s3_manager.upload_file(defaults.bucket_name, current_folder + "/mapreduce/mapper.py", defaults.scripts_remote_path + defaults.step_mapper_script, _upload_mapper_callback)
    # wait until finished
    while not mapper_uploaded:
        time.sleep(1)
    logging.info("Mapper script uploaded")

def _upload_mapper_callback(transmitted, total):
    global mapper_uploaded
    logging.info("Upload mapper callback. Transmitted: " + str(transmitted) + " - total: " + str(total))
    # only called once
    mapper_uploaded = True

# upload mr.jar to s3 bucket
def upload_jar_to_s3_bucket():
    global jar_uploaded
    logging.info("Uploading jar file")
    current_folder = os.path.dirname(os.path.realpath(__file__))
    s3_manager.upload_file(defaults.bucket_name, current_folder + "/mapreduce/mr.jar", defaults.scripts_remote_path + "mr.jar", _upload_jar_callback)
    # wait until finished
    while not jar_uploaded:
        time.sleep(1)
    logging.info("Jar uploaded")

def _upload_jar_callback(transmitted, total):
    global jar_uploaded
    logging.info("Upload jar callback. Transmitted: " + str(transmitted) + " - total: " + str(total))
    # only called once
    jar_uploaded = True

# upload copy_to_local.sh script to s3 bucket
def upload_copy_to_local_to_s3_bucket():
    global copy_to_local_uploaded
    logging.info("Uploading copy_to_local script")
    current_folder = os.path.dirname(os.path.realpath(__file__))
    s3_manager.upload_file(defaults.bucket_name, current_folder + "/bash/copy_to_local.sh", defaults.scripts_remote_path + defaults.step_copy_to_local_script, _upload_copy_to_local_callback)
    # wait until finished
    while not copy_to_local_uploaded:
        time.sleep(1)
    logging.info("Copy_to_local script uploaded")

def _upload_copy_to_local_callback(transmitted, total):
    global copy_to_local_uploaded
    logging.info("Upload copy_to_local callback. Transmitted: " + str(transmitted) + " - total: " + str(total))
    # only called once
    copy_to_local_uploaded = True

# start EMR cluster
def launch_emr_cluster():
    logging.info("Launching EMR Cluster with name: " + defaults.emr_cluster_name)
    defaults.cluster_id = emr_manager.launch_cluster(defaults.master_type, defaults.slave_type, defaults.num_instances, defaults.ami_version)
    logging.info("EMR Cluster launched: " + defaults.cluster_id)

# run copy_to_local.sh step in EMR cluster
def run_copy_to_local_step():
    logging.info("Running copy_to_local step in cluster: " + defaults.cluster_id + "with script path: " + defaults.step_copy_to_local_s3)
    defaults.step_id = emr_manager.run_scripting_step(name = defaults.step_name,
                                            cluster_id = defaults.cluster_id,
                                            script_path = defaults.step_copy_to_local_s3)
    logging.info("Scripting step " + defaults.step_id + " completed in cluster " + defaults.cluster_id)

# run mapreduce in EMR cluster
def run_mapreduce():
    logging.info("Running MapReduce " + defaults.step_type + " step in cluster: " + defaults.cluster_id)
    run_step = run_streaming_mapreduce
    if (defaults.step_type == "jar"):
        run_step = run_jar_mapreduce
    defaults.step_id = run_step();
    logging.info(defaults.step_type + " step " + defaults.step_id + " completed in cluster " + defaults.cluster_id)

def run_streaming_mapreduce():
    return emr_manager.run_streaming_step(name = defaults.step_name,
                                            cluster_id = defaults.cluster_id,
                                            mapper_path = defaults.step_mapper,
                                            reducer_path = defaults.step_reducer,
                                            input_path = defaults.step_input,
                                            output_path = defaults.step_output)

def run_jar_mapreduce():
    return emr_manager.run_jar_step(name = defaults.step_name,
                                    cluster_id = defaults.cluster_id,
                                    jar_path = defaults.step_jar_path,
                                    class_name = defaults.step_jar_class_name,
                                    input_path = defaults.step_input,
                                    output_path = defaults.step_output)

# terminate EMR cluster
def terminate_emr_cluster():
    logging.info("Terminating cluster: " + defaults.cluster_id)
    emr_manager.terminate_cluster(defaults.cluster_id)
    logging.info("Cluster terminated: " + defaults.cluster_id)

# create redshift_table
def create_redshift_table():
    logging.info("Creating table " + defaults.table_name + " in Redshift")
    try:
        redshift_cursor.execute("CREATE TABLE " + defaults.table_name + " \
            ( \
              request_date     VARCHAR(10) NOT NULL,\
              destination      VARCHAR(1000) NOT NULL,\
              days_advance     INTEGER NOT NULL,\
              hotels_returned  INTEGER NOT NULL\
            )")
        logging.info("Table " + defaults.table_name + " created in Redshift")
    except Error as error:
        logging.error("Something went wrong while creating " + defaults.table_name + " table.")
        logging.error(error.pgerror)
        logging.error(error.diag.message_detail)

# copy contents of output to Redshift
def copy_output_to_redshift():
    logging.info("Copying output into Redshift. This might take time. Check progress in your AWS Console")
    try:
        s3_source = "s3://" + defaults.bucket_name + defaults.output_remote_path + "part-"
        credentials = "CREDENTIALS 'aws_access_key_id=" + defaults.access_key + ";aws_secret_access_key=" + defaults.secret_key + "'"
        redshift_cursor.execute("COPY " + defaults.table_name + " FROM '" + s3_source + "' " + credentials + " DELIMITER '|' MAXERROR 10")
    except Error as error:
        logging.error("Something went wrong while copying data to " + defaults.table_name + " table.")
        logging.error(error.pgerror)
        logging.error(error.diag.message_detail)

# vacuum redshift
def vacuum_redshift():
    logging.info("Vacuuming database")
    try:
        redshift_cursor.execute("vacuum")
        logging.info("Database vacuumed")
    except Error as error:
        logging.error("Something went wrong while vacuuming table.")
        logging.error(error.pgerror)
        logging.error(error.diag.message_detail)

# analyze redshift
def analyze_redshift():
    logging.info("Analyzing database")
    try:
        redshift_cursor.execute("analyze")
        logging.info("Database analyzed")
    except Error as error:
        logging.error("Something went wrong while analyzing database.")
        logging.error(error.pgerror)
        logging.error(error.diag.message_detail)

# delete redshift_table
def delete_redshift_table():
    logging.info("Deleting contents of table " + defaults.table_name + " in Redshift")
    try:
        redshift_cursor.execute("DELETE FROM " + defaults.table_name)
        logging.info("Contents of table " + defaults.table_name + " deleted in Redshift")
    except Error as error:
        logging.error("Something went wrong while deleting " + defaults.table_name + " table.")
        logging.error(error.pgerror)
        logging.error(error.diag.message_detail)

# drop redshift_table
def drop_redshift_table():
    logging.info("Dropping table " + defaults.table_name + " in Redshift")
    try:
        redshift_cursor.execute("DROP TABLE " + defaults.table_name)
        logging.info("Table " + defaults.table_name + " was removed from Redshift")
    except Error as error:
        logging.error("Something went wrong while dropping " + defaults.table_name + " table.")
        logging.error(error.pgerror)
        logging.error(error.diag.message_detail)

# deletes output from bucket
def delete_output_from_bucket():
    logging.info("Deleting output in bucket: " + defaults.bucket_name)
    s3_manager.delete_files_with_prefix(defaults.bucket_name, defaults.output_remote_path[1:])
    logging.info("Output deleted in bucket: " + defaults.bucket_name)

# empty bucket
def empty_bucket():
    logging.info("Emptying bucket: " + defaults.bucket_name)
    s3_manager.empty_bucket(defaults.bucket_name)
    logging.info("Bucket empty: " + defaults.bucket_name)

# the orchestrator
orchestrator = {
    "create_bucket": create_s3_bucket,
    "upload_files": upload_files_to_s3_bucket,
    "upload_mapper": upload_mapper_to_s3_bucket,
    "upload_copy_to_local": upload_copy_to_local_to_s3_bucket,
    "upload_jar": upload_jar_to_s3_bucket,
    "launch_emr": launch_emr_cluster,
    "copy_to_local": run_copy_to_local_step,
    "mapreduce": run_mapreduce,
    "terminate_emr": terminate_emr_cluster,
    "create_redshift_table": create_redshift_table,
    "copy_output_to_redshift": copy_output_to_redshift,
    "delete_redshift_table": delete_redshift_table,
    "drop_redshift_table": drop_redshift_table,
    "empty_bucket": empty_bucket,
    "vacuum_redshift": vacuum_redshift,
    "analyze_redshift": analyze_redshift,
    "delete_output": delete_output_from_bucket
}

if __name__ == '__main__':
    if len(sys.argv) > 1:
        for action in sys.argv[1:]:
            if action in orchestrator:
                orchestrator[action]()
