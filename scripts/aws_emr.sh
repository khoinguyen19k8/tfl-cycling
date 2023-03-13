# When done with work
sudo aws s3 cp --recursive --exclude "*" --include "*.zpln" /var/lib/zeppelin/notebook/ s3://tfl-cycling/emr_testing/notebook
sudo aws s3 cp /usr/lib/zeppelin/conf/notebook-authorization.json s3://tfl-cycling/emr_testing/config/notebook-authorization.json

#Boostrapping the cluster
sudo aws s3 cp --recursive --exclude "*" --include "*.zpln" s3://tfl-cycling/emr_testing/notebook/ /var/lib/zeppelin/notebook/
sudo aws s3 cp s3://tfl-cycling/emr_testing/config/notebook-authorization.json /usr/lib/zeppelin/conf/notebook-authorization.json