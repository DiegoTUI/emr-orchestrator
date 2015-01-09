#bin/bash -xe
hadoop fs -copyToLocal s3://tuiinnovation-emr/scripts/mapper.py /home/hadoop/
chmod +x /home/hadoop/mapper.py