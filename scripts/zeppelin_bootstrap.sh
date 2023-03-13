#! /bin/bash
if grep isMaster /mnt/var/lib/info/instance.json | grep false;
then        
    echo "This is not master node, do nothing,exiting"
    exit 0
fi
echo "This is master, continuing to execute script"

aws s3 cp --recursive --exclude "*" --include "*.zpln" s3://tfl-cycling/emr_testing/notebook/ /var/lib/zeppelin/notebook/
aws s3 cp s3://tfl-cycling/emr_testing/config/notebook-authorization.json /usr/lib/zeppelin/conf/notebook-authorization.json