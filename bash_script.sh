#!/bin/bash

# Initiate Variable
BUCKET_NAME=gs://academi
BQ_DATASET=academi-315713:dwh
CLUSTER_NAME=dataproc-pyspark
REGION=asia-south1
DATE=$(date -d 'April 26' +%Y-%m-%d)

# Copy from local to GCS
# a=1
# for file in ./data/*.json; do
#     file_date=$(date -d "$DATE +$a days" +%Y-%m-%d)
#     gsutil cp $file "$BUCKET_NAME/$file_date.json"
#     a=$[$a+1]
# done


# Enable google cloud services
# gcloud services enable compute.googleapis.com \
#     dataproc.googleapis.com \
#     bigquerystorage.googleapis.com

# Create Dataproc Cluster
# gcloud dataproc clusters create $CLUSTER_NAME \
#     --region=$REGION \
#     --master-machine-type n1-standard-2 \
#     --master-boot-disk-size 20 \
#     --num-workers 2 \
#     --worker-machine-type n1-standard-2 \
#     --worker-boot-disk-size 20 \
#     --image-version 1.3

# Submit Spark Job
gcloud dataproc jobs submit pyspark spark_job.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \

