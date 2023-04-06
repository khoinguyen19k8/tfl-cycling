#! /bin/bash

aws s3 cp s3://cycling.data.tfl.gov.uk/usage-stats/ s3://tfl-cycling/usage-stats/ --copy-props none --recursive --exclude "*" --include "*.csv" --include "*.xlsx"

aws s3 sync s3://cycling.data.tfl.gov.uk/usage-stats/ s3://tfl-cycling/usage-stats/ --exclude "*" --include "*.csv" --dryrun