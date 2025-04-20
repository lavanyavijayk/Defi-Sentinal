#! /bin/bash

export PROJECT_ID=""
export REGION=""
export IMAGE_NAME=""
export IMAGE_TAG="V1"
export IMAGE_URI="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"
export TEMPLATE_PATH="gs://${PROJECT_ID}-dataflow-bucket/templates/dataflow_job_template.json"

#Building and pushing docker image to GCR
docker build --platform linux/amd64,linux/arm64 -t ${IMAGE_URI} .
docker push ${IMAGE_URI}

#Creating template and uploading
gcloud dataflow flex-template build ${TEMPLATE_PATH} \
    --image "${IMAGE_URI}" \
    --sdk-language "PYTHON" \
    --enable-streaming-engine
