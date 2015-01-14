import time
import logging
import sys
from boto.emr.connection import EmrConnection
from boto.emr.bootstrap_action import BootstrapAction
from boto.regioninfo import RegionInfo
from boto.emr.step import StreamingStep
from boto.emr.step import ScriptRunnerStep
from boto.emr.step import JarStep
 
#Class for launching an EMR cluster
 
class EmrManager(object):
 
    # Default constructor of the class. Uses default parameters if not provided.
    def __init__(self, parameters):
        try: 
            self.region_name = parameters["region_name"]
            self.access_key = parameters["access_key"]
            self.secret_key = parameters["secret_key"]
            self.ec2_keypair_name = parameters["ec2_keypair_name"]
            self.base_bucket = parameters["base_bucket"]
            self.log_dir = parameters["log_dir"]
            self.emr_status_wait = parameters["emr_status_wait"]
            self.step_status_wait = parameters["step_status_wait"]
            self.emr_cluster_name = parameters["emr_cluster_name"]
        except:
            logging.error("Something went wrong initializing EmrManager")
            sys.exit()

        # Establishing EmrConnection
        self.connection = EmrConnection(self.access_key, self.secret_key,
                             region=RegionInfo(name=self.region_name,
                             endpoint=self.region_name + '.elasticmapreduce.amazonaws.com'))

        self.log_bucket_name = self.base_bucket + self.log_dir
 
    #Method for launching the EMR cluster
    def launch_cluster(self, master_type, slave_type, num_instances, ami_version):
        try:
            #Launching the cluster
            cluster_id = self.connection.run_jobflow(
                             self.emr_cluster_name,
                             self.log_bucket_name,
                             ec2_keyname=self.ec2_keypair_name,
                             keep_alive=True,
                             action_on_failure = 'CANCEL_AND_WAIT',
                             master_instance_type=master_type,
                             slave_instance_type=slave_type,
                             num_instances=num_instances,
                             ami_version=ami_version)

            logging.info("Launching cluster: " + cluster_id + ". Please be patient. Check the status of your cluster in your AWS Console")

            # Checking the state of EMR cluster
            state = self.connection.describe_jobflow(cluster_id).state
            while state != u'COMPLETED' and state != u'SHUTTING_DOWN' and state != u'FAILED' and state != u'WAITING':
                #sleeping to recheck for status.
                time.sleep(int(self.emr_status_wait))
                state = self.connection.describe_jobflow(cluster_id).state
                logging.info("Creating cluster " + cluster_id + ". Status: " + state)
 
            if state == u'SHUTTING_DOWN' or state == u'FAILED':
                logging.error("Launching EMR cluster failed")
                return "ERROR"
 
            #Check if the state is WAITING. Then launch the next steps
            if state == u'WAITING':
                #Finding the master node dns of EMR cluster
                master_dns = self.connection.describe_jobflow(cluster_id).masterpublicdnsname
                logging.info("Launched EMR Cluster Successfully with cluster id:" + cluster_id)
                logging.info("Master node DNS of EMR " + master_dns)
                return cluster_id
        except:
            logging.error("Launching EMR cluster failed")
            return "FAILED"

    # run scripting step in cluster
    def run_scripting_step(self, cluster_id, name, script_path):
        try:
            step = ScriptRunnerStep(name=name, 
                                    step_args=[script_path],
                                    action_on_failure="CONTINUE")
            return self._run_step(cluster_id, step)
        except:
            logging.error("Running scripting step in cluster " + cluster_id + " failed.")
            return "FAILED"

    # run streaming step in cluster
    def run_streaming_step(self, cluster_id, name, mapper_path, reducer_path, input_path, output_path):
        try:
            # bundle files with the job
            files = []
            if mapper_path != "NONE":
                files.append(mapper_path)
                mapper_path = mapper_path.split("/")[-1]
            if reducer_path != "NONE":
                files.append(reducer_path)
                reducer_path = reducer_path.split("/")[-1]
            # build streaming step
            logging.debug("Launching streaming step with mapper: " + mapper_path + " reducer: " + reducer_path + " and files: " + str(files))
            step = StreamingStep(name=name,
                                    step_args=["-files"] + files, 
                                    mapper=mapper_path, 
                                    reducer=reducer_path, 
                                    input=input_path, 
                                    output=output_path, 
                                    action_on_failure="CONTINUE")
            return self._run_step(cluster_id, step)            
        except:
            logging.error("Running streaming step in cluster " + cluster_id + " failed.")
            return "FAILED"

    # run mapreduce jar step in cluster
    def run_jar_step(self, cluster_id, name, jar_path, class_name, input_path, output_path):
        try:
            # build streaming step
            logging.debug("Launching jar step with jar: " + jar_path + " class name: " + class_name + " input: " + input_path + " and output: " + output_path)
            step = JarStep(name=name,
                            jar=jar_path, 
                            step_args= [class_name,
                                        input_path,
                                        output_path])
            return self._run_step(cluster_id, step)            
        except:
            logging.error("Running jar step in cluster " + cluster_id + " failed.")
            return "FAILED"

    def _run_step(self, cluster_id, step):
        step_list = self.connection.add_jobflow_steps(cluster_id, [step])
        step_id = step_list.stepids[0].value

        logging.info("Starting step " + step_id + " in cluster " + cluster_id + ". Please be patient. Check the progress of the job in your AWS Console")

        # Checking the state of the step
        state = self._find_step_state(cluster_id, step_id)
        while state != u'NOT_FOUND' and state != u'ERROR' and state != u'FAILED' and state!=u'COMPLETED':
            #sleeping to recheck for status.
            time.sleep(int(self.step_status_wait))
            state = self._find_step_state(cluster_id, step_id)
            logging.info("Starting step " + step_id + " in cluster " + cluster_id + ". Status: " + state)

        if state == u'FAILED':
            logging.error("Step " + step_id + " failed in cluster: " + cluster_id)
            return "FAILED"
        if state == u'NOT_FOUND':
            logging.error("Step " + step_id + " could not be found in cluster: " + cluster_id)
            return "NOT_FOUND"
        if state == u'ERROR':
            logging.error("Step " + step_id + " produced an error in _find_step_state in cluster: " + cluster_id)
            return "ERROR"

        #Check if the state is WAITING. Then launch the next steps
        if state == u'COMPLETED':
            #Finding the master node dns of EMR cluster
            logging.info("Step " + step_id + " succesfully completed in cluster: " + cluster_id)
            return step_id


    def _find_step_state(self, cluster_id, step_id):
        try:
            step_summary_list = self.connection.list_steps(cluster_id)
            for step_summary in step_summary_list.steps:
                if step_summary.id == step_id:
                    return step_summary.status.state
            return "NOT_FOUND"
        except:
            return "ERROR"

    #Method for terminating the EMR cluster
    def terminate_cluster(self, cluster_id):
        self.connection.terminate_jobflow(cluster_id)
