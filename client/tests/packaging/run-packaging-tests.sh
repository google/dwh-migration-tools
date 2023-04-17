#!/bin/bash -e

for BASE_OS in RedHat Ubuntu; do
   for ICU_ENABLED in false true; do
        docker build \
            -t dwh-migration-tools-test \
            -f Dockerfile-$BASE_OS \
            --build-arg ICU_ENABLED=$ICU_ENABLED \
            ../..
        docker run -it --rm \
            -v ~/.config/gcloud:/root/.config/gcloud \
            -e BQMS_PROJECT=$BQMS_PROJECT \
            -e BQMS_GCS_BUCKET=$BQMS_GCS_BUCKET \
            dwh-migration-tools-test
   done
done
