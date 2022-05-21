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

import os
from os.path import abspath, basename, isdir, join

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.storage import Bucket


def upload_directory(local_dir: str, bucket_name: str, gcs_path: str):
    """Uploads all the files from a local directory to a gcs bucket.

    Args:
      local_dir: path to the local directory.
      bucket_name: name of the gcs bucket.  If the bucket doesn't exist, the method tries to create one.
      gcs_path: the path to the gcs directory that stores the files.
    """
    assert isdir(local_dir), "Can't find input directory %s." % local_dir
    client = storage.Client()

    try:
        print("Get bucket %s" % bucket_name)
        bucket: Bucket = client.get_bucket(bucket_name)
    except NotFound:
        print('The bucket "%s" does not exist, creating one...' % bucket_name)
        bucket = client.create_bucket(bucket_name)

    dir_abs_path = abspath(local_dir)
    for (root, dirs, files) in os.walk(dir_abs_path):
        for name in files:
            sub_dir = root[len(dir_abs_path) :]
            if sub_dir.startswith("/"):
                sub_dir = sub_dir[1:]
            file_path = join(root, name)
            print('Uploading file "%s" to gcs...' % file_path)
            gcs_file_path = join(gcs_path, sub_dir, name)
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(file_path)
    print('Finished uploading input files to gcs "%s/%s".' % (bucket_name, gcs_path))


def download_directory(local_dir: str, bucket_name: str, gcs_path: str):
    """Download all the files from a gcs bucket to a local directory.

    Args:
        local_dir: path to the local directory to store the downloaded files. It will create the directory if it
            doesn't exist.
        bucket_name: name of the gcs bucket.
        gcs_path: the path to the gcs directory that stores the files.
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=gcs_path)
    print('Start downloading outputs from gcs "%s/%s"' % (bucket_name, gcs_path))
    for blob in blobs:
        file_name = basename(blob.name)
        sub_dir = blob.name[len(gcs_path) + 1 : -len(file_name)]
        file_dir = join(local_dir, sub_dir)
        os.makedirs(file_dir, exist_ok=True)
        file_path = join(file_dir, file_name)
        print('Downloading output file to "%s"...' % file_path)
        blob.download_to_filename(file_path)

    print('Finished downloading. Output files are in "%s".' % local_dir)
