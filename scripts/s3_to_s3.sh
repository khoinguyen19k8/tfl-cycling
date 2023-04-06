#! /bin/bash

aws s3 cp s3://cycling.data.tfl.gov.uk/usage-stats/ s3://tfl-cycling/raw/ --copy-props none --recursive --exclude "*" --include "*.csv" --include "*.xlsx"