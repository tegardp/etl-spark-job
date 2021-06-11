#!/bin/bash

# Initiate Variable
BUCKET_NAME=gs://academi
CLUSTER_NAME=dataproc-pyspark
TEMPLATE_ID=academi
REGION=asia-south1

gsutil cp ./spark-job/extract_transform.py "$BUCKET_NAME/spark-job/extract_transform.py"
gsutil cp ./spark-job/bq_to_gcs.py "$BUCKET_NAME/spark-job/bq_to_gcs.py"

# # Copy from local to GCS
# counter=1
# DATE=$(date -d 'April 26' +%Y-%m-%d)
# for file in ./data/*.json; do
#     file_date=$(date -d "$DATE +$counter days" +%Y-%m-%d)
#     gsutil cp $file "$BUCKET_NAME/data/$file_date.json"
#     counter=$[$counter+1]
# done


#########################################
# Using Google Dataproc Cluster         #
#########################################

# # Enable google cloud services
# gcloud services enable compute.googleapis.com \
#     dataproc.googleapis.com \
#     bigquerystorage.googleapis.com

# # Create Dataproc Cluster
# gcloud dataproc clusters create $CLUSTER_NAME \
#     --region=$REGION \
#     --master-machine-type n1-standard-2 \
#     --master-boot-disk-size 20 \
#     --num-workers 2 \
#     --worker-machine-type n1-standard-2 \
#     --worker-boot-disk-size 20 \
#     --image-version 1.3

# # Submit Spark Job
# gcloud dataproc jobs submit pyspark "$BUCKET_NAME/spark-job/extract_transform.py" \
#     --cluster=$CLUSTER_NAME \
#     --region=$REGION \
#     --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar

# gcloud dataproc jobs submit pyspark "$BUCKET_NAME/spark-job/bq_to_gcs.py" \
#     --cluster=$CLUSTER_NAME \
#     --region=$REGION \
#     --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar

# # Delete Dataproc Cluster
# gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION


###########################################
# Using Google Dataproc Workflow Template #
###########################################
gcloud dataproc workflow-templates create $TEMPLATE_ID \
    --region=$REGION

gcloud dataproc workflow-templates set-managed-cluster $TEMPLATE_ID \
    --region=$REGION \
    --master-machine-type n1-standard-2 \
    --master-boot-disk-size 35 \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size 35 \
    --num-workers=2 \
    --cluster-name=$CLUSTER_NAME
    --image-version=1.5-ubuntu18 \
    
gcloud dataproc workflow-templates add-job pyspark "$BUCKET_NAME/spark-job/extract_transform.py" \
    --region=$REGION \
    --step-id=extract-transform \
    --workflow-template=$TEMPLATE_ID \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

gcloud dataproc workflow-templates add-job pyspark "$BUCKET_NAME/spark-job/bq_to_gcs.py" \
    --region=$REGION \
    --step-id=bq-to-gcs \
    --start-after=extract-transform \
    --workflow-template=$TEMPLATE_ID \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

gcloud beta dataproc workflow-templates instantiate $TEMPLATE_ID \
    --region=$REGION