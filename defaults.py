# default parameters

# AWS credentials
region_name = "eu-west-1"
access_key = "ACCESS_KEY_HERE"
secret_key = "SECRET_KEY_HERE"

# S3
bucket_name = "tuiinnovation-emr"
scripts_remote_path = "/scripts/"
input_local_path = "./input/"
output_remote_path = "/output/"

# Elastic MapReduce
ec2_keypair_name = "innovationLab"
master_type = "m3.xlarge"
slave_type = "m1.large"
num_instances = 10
ami_version = "3.3.1"
log_dir = "/logs"
emr_status_wait = 20
step_status_wait = 20
emr_cluster_name = "suppliers-integration-emr"
cluster_id = None
step_type = "jar"
# Streaming/Jar step
step_id = None
step_name = "TestStep"
step_jar_path = "s3n://" + bucket_name + scripts_remote_path + "mr.jar"
step_jar_class_name = "MapReduce"
step_mapper_script = "mapper.py"
step_mapper = "s3n://" + bucket_name + scripts_remote_path + step_mapper_script
step_copy_to_local_script = "copy_to_local.sh"
step_copy_to_local_s3 = "s3n://" + bucket_name + scripts_remote_path + step_copy_to_local_script
step_reducer = "NONE"
step_input = "s3n://" + bucket_name + "/input/SuppliersMonitor.log-20140524.bz2"
#step_input = "s3n://" + bucket_name + "/input/"
step_output = "s3n://" + bucket_name + output_remote_path
# Redshift
db_name = "tuiinnovationredshift"
db_user = "dlafuente"
db_password = "SonFangos100"
db_port = "5439"
db_host = "tuiinnovation.ccxabt6pla67.eu-west-1.redshift.amazonaws.com"
table_name = "suppliers"
