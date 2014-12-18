import logging
from boto.emr.step import StreamingStep
from emr_manager import EmrManager

# script to trigger a test step process
logging.getLogger().setLevel(logging.INFO)
manager = EmrManager()
step = StreamingStep(name="testStep", mapper="s3n://tuiinnovation-holycrap/scripts/mapper.py", reducer="NONE", input="s3n://tuiinnovation-emr/input/SuppliersMonitor.log-20140524.bz2", output="s3n://tuiinnovation-emr/output/", action_on_failure="CONTINUE")
cluster_id = "j-1OSKIJWRXGEJW"
step_id = manager.run_step(cluster_id, step)
logging.info("step " + step_id + " completed in " + cluster_id)