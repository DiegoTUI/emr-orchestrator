import logging
import sys
import os
import time
import defaults
from boto.emr.step import StreamingStep
from aws.s3_manager import S3Manager
from aws.emr_manager import EmrManager

# Logger level
logging.getLogger().setLevel(logging.INFO)

# script to trigger the whole process

# globals
s3_parameters = {
    "access_key": defaults.access_key,
    "secret_key": defaults.secret_key
}
s3_manager = S3Manager(s3_parameters)
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
mapper_uploaded = False

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

# start EMR cluster
def launch_emr_cluster():
    logging.info("Launching EMR Cluster with name: " + defaults.emr_cluster_name)
    defaults.cluster_id = emr_manager.launch_cluster(defaults.master_type, defaults.slave_type, defaults.num_instances, defaults.ami_version)
    logging.info("EMR Cluster launched: " + defaults.cluster_id)

# run mapreduce in EMR cluster
def run_mapreduce():
    logging.info("Running MapReduce step in cluster: " + defaults.cluster_id)
    defaults.step_id = emr_manager.run_step(name = defaults.step_name,
                                            cluster_id = defaults.cluster_id,
                                            mapper_path = defaults.step_mapper,
                                            reducer_path = defaults.step_reducer,
                                            input_path = defaults.step_input,
                                            output_path = defaults.step_output)
    logging.info("Step " + defaults.step_id + " completed in cluster " + defaults.cluster_id)

# terminate EMR cluster
def terminate_emr_cluster():
    logging.info("Terminating cluster: " + defaults.cluster_id)
    emr_manager.terminate_cluster(defaults.cluster_id)
    logging.info("Cluster terminated: " + defaults.cluster_id)

# copy contents of output to Redshift

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
    "launch_emr": launch_emr_cluster,
    "mapreduce": run_mapreduce,
    "terminate_emr": terminate_emr_cluster,
    "empty_bucket": empty_bucket
}

if __name__ == '__main__':
    if len(sys.argv) > 1:
        for action in sys.argv[1:]:
            if action in orchestrator:
                orchestrator[action]()
