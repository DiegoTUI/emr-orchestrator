# default parameters

# AWS credentials
region_name = "eu-west-1"
access_key = "AKIAIDTHFEIDNEQKB4TA"
secret_key = "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"

# S3
#bucket_name = "tuiinnovation-emr"
bucket_name = "tuiinnovation-holycrap"
scripts_remote_path = "/scripts/"
input_local_path = "./input/"

# Elastic MapReduce
ec2_keypair_name = "innovationLab"
master_type = "m3.xlarge"
slave_type = "m1.large"
num_instances = 3
ami_version = "2.4.2"
log_dir = "/logs"
emr_status_wait = 20
step_status_wait = 60
emr_cluster_name = "suppliers-integration-emr"
#cluster_id = None
cluster_id = "j-YOBTHKC7EG7"
# Streaming step
step_id = None
step_name = "TestStep"
step_mapper_script = "mapper.py"
step_mapper = "s3n://" + bucket_name + scripts_remote_path + step_mapper_script
step_reducer = "NONE"
step_input = "s3n://tuiinnovation-emr/input/SuppliersMonitor.log-20140524.bz2"
step_output = "s3n://tuiinnovation-emr/output/"