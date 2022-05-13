# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os

from google.cloud.exceptions import NotFound
from google.cloud import storage
from google.cloud.storage import Bucket


def upload_directory(local_dir: str, bucket_name: str, gcs_path: str):
    """Uploads all the files from a local directory to a gcs bucket.

    Args:
      local_dir: path to the local directory.
      bucket_name: name of the gcs bucket.  If the bucket doesn't exist, the method tries to create one.
      gcs_path: the path to the gcs directory that stores the files.
    """
    assert os.path.isdir(local_dir), "Can't find input directory %s." % local_dir
    client = storage.Client()

    try:
        print("Get bucket %s" % bucket_name)
        bucket: Bucket = client.get_bucket(bucket_name)
    except NotFound:
        print("The bucket \"%s\" does not exist, creating one..." % bucket_name)
        bucket = client.create_bucket(bucket_name)

    for file in glob.glob(os.path.join(local_dir, "*")):
        if os.path.isfile(file):
            print("Uploading file \"%s\" to gcs..." % file)
            gcs_file_path = os.path.join(gcs_path, os.path.basename(file))
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(file)
    print("Finished uploading input files to gcs \"%s/%s\"." % (bucket_name, gcs_path))


def download_directory(local_dir: str, bucket_name: str, gcs_path: str):
    """Download all the files from a gcs bucket to a local directory.

    Args:
        local_dir: path to the local directory to store the downloaded files. It will create the directory if it
            doesn't exist.
        bucket_name: name of the gcs bucket.
        gcs_path: the path to the gcs directory that stores the files.
    """
    os.makedirs(local_dir, exist_ok=True)
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=gcs_path)
    print("Start downloading outputs from gcs \"%s/%s\"" % (bucket_name, gcs_path))
    for blob in blobs:
        file_name = os.path.join(local_dir, os.path.basename(blob.name))
        print("Downloading output file to \"%s\"..." % file_name)
        blob.download_to_filename(file_name)

    print("Finished downloading. Output files are in \"%s\"." % local_dir)
