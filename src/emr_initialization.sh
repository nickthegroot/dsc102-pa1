#! /bin/bash

sudo yum -y update
sudo yum -y install git

git clone https://github.com/nickthegroot/dsc102-pa1.git pa1
cd pa1

spark-submit src/label_prep.py \
    "s3://dsc102-nickdegroot-scratch/data/historical_data_time_2017*.txt" \
    "s3://dsc102-nickdegroot-scratch/2017-labels.parquet"