#!/bin/bash

# sudo apt-get update
# sudo apt-get install bazel
# sudo apt-get install \
#     libxml2-dev \
#     build-essential \
#     python3-dev

defaultWorkspace="gen-v2alpha-py-client"
workspace=$defaultWorkspace


while getopts w:g: flag
do
    case "${flag}" in
        w) workspace=${OPTARG};;
        # e.g. "~/Desktop/git-things/dwh-migration-tools/"
        g) gitDirectory=${OPTARG};;
    esac
done

# Get to or create inital workspace 
if [ "$workspace" = "$defaultWorkspace" ]; then
    echo "Generating default workspace"
    cd $(p4 g4d -f "$defaultWorkspace")
else
    echo "Going to specific workspace: '"$workspace"'"
    cd $(p4 g4d "$workspace")
fi

echo "Running api_publish_changelist for //google/cloud/bigquery/migration:bigquerymigration_v2alpha_public_proto_gen"
blaze run "//google/cloud/bigquery/migration:bigquerymigration_v2alpha_public_proto_gen" -- --local

cd third_party/googleapis/stable/

echo "Generating Bazel build files"
bazel run //:build_gen -- --src="google/cloud/bigquery/migration/v2alpha"

echo "Building client libraries"
bazel build "//google/cloud/bigquery/migration/v2alpha/..."

echo "Removing old client library"
rm -rf "$gitDirectory"client/google/

echo "Copying new client lib to git"

tar -xzpf bazel-bin/google/cloud/bigquery/migration/v2alpha/bigquery-migration-v2alpha-py.tar.gz -C /tmp/

cp -r /tmp/bigquery-migration-v2alpha-py/google/ "$gitDirectory"client/

# Clean up workspace at the end if it was generated
# if [ "$workspace" = "$defaultWorkspace" ]; then
#     echo "Deleting generated workspace"
#     p4 revert -c default
#     p4 citc -d "$defaultWorkspace"
# fi