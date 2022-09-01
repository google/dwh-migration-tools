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
"""Utility to upload to and download from GCS."""

import logging
import os
from os.path import abspath, basename, isdir, join
from typing import List, OrderedDict
from pathlib import Path

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.storage import Bucket


def upload_directory(local_dir: str, bucket_name: str, gcs_path: str) -> None:
    """Uploads all the files from a local directory to a gcs bucket.

    Args:
      local_dir: path to the local directory.
      bucket_name: name of the gcs bucket.  If the bucket doesn't exist, the method
        tries to create one.
      gcs_path: the path to the gcs directory that stores the files.
    """
    assert isdir(local_dir), f"Can't find input directory {local_dir}."
    client = storage.Client()

    try:
        logging.info("Get bucket %s", bucket_name)
        bucket: Bucket = client.get_bucket(bucket_name)
    except NotFound:
        logging.info('The bucket "%s" does not exist, creating one...', bucket_name)
        bucket = client.create_bucket(bucket_name)

    dir_abs_path = abspath(local_dir)
    for root, _, files in os.walk(dir_abs_path):
        for name in files:
            sub_dir = root[len(dir_abs_path) :]
            if sub_dir.startswith("/"):
                sub_dir = sub_dir[1:]
            file_path = join(root, name)
            logging.info('Uploading file "%s" to gcs...', file_path)
            gcs_file_path = join(gcs_path, sub_dir, name)
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(file_path)
    logging.info(
        'Finished uploading input files to gcs "%s/%s".', bucket_name, gcs_path
    )


def upload_full_directories(
    local_dir_list: List[str], bucket_name: str, gcs_path: str
) -> None:
    """Uploads all the files from local directories to a gcs bucket using the full paths.

    Args:
      local_dir_list: paths to the local directories
      bucket_name: name of the gcs bucket.  If the bucket doesn't exist, the method
        tries to create one.
      gcs_path: the path to the gcs directory that stores the files.
    """
    for local_dir in local_dir_list:
        upload_full_directory(local_dir, bucket_name, gcs_path)


def upload_full_directory(local_dir: str, bucket_name: str, gcs_path: str) -> None:
    """Uploads all the files from a local directory to a gcs bucket using the full/absolute file path.

    Args:
      local_dir: path to the local directory.
      bucket_name: name of the gcs bucket.  If the bucket doesn't exist, the method
        tries to create one.
      gcs_path: the path to the gcs directory that stores the files.
    """
    assert isdir(local_dir), f"Can't find input directory {local_dir}."
    client = storage.Client()

    try:
        logging.info("Get bucket %s", bucket_name)
        bucket: Bucket = client.get_bucket(bucket_name)
    except NotFound:
        logging.info('The bucket "%s" does not exist, creating one...', bucket_name)
        bucket = client.create_bucket(bucket_name)

    dir_abs_path = abspath(local_dir)
    for root, _, files in os.walk(dir_abs_path):
        for name in files:
            sub_dir = root[len(dir_abs_path) :]
            if sub_dir.startswith("/"):
                sub_dir = sub_dir[1:]
            file_path = join(root, name)
            logging.info('Uploading file "%s" to gcs...', file_path)
            gcs_file_path = join(join(gcs_path, file_path[1:]))
            blob = bucket.blob(gcs_file_path)
            blob.upload_from_filename(file_path)
    logging.info(
        'Finished uploading input files to gcs "%s/%s".', bucket_name, gcs_path
    )


def download_directory(local_dir: str, bucket_name: str, gcs_path: str) -> None:
    """Download all the files from a gcs bucket to a local directory.

    Args:
        local_dir: path to the local directory to store the downloaded files. It will
            create the directory if it doesn't exist.
        bucket_name: name of the gcs bucket.
        gcs_path: the path to the gcs directory that stores the files.
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=gcs_path)
    logging.info('Start downloading outputs from gcs "%s/%s"', bucket_name, gcs_path)
    for blob in blobs:
        file_name = basename(blob.name)
        sub_dir = blob.name[len(gcs_path) + 1 : -len(file_name)]
        file_dir = join(local_dir, sub_dir)
        os.makedirs(file_dir, exist_ok=True)
        file_path = join(file_dir, file_name)
        logging.info('Downloading output file to "%s"...', file_path)
        blob.download_to_filename(file_path)

    logging.info('Finished downloading. Output files are in "%s".', local_dir)


def download_directories(
    local_dir_map: OrderedDict[str, str], bucket_name: str, gcs_path: str
) -> None:
    """Download all the files from specific directories in a gcs bucket to local directories.

    Args:
        local_dir_map: paths from the input locations to the local directories to store the downloaded files. It will
            create the directory if it doesn't exist.
        bucket_name: name of the gcs bucket.
        gcs_path: the path to the gcs directory that stores the files.
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=gcs_path)
    logging.info('Start downloading outputs from gcs "%s/%s"', bucket_name, gcs_path)
    for blob in blobs:
        file_name = basename(blob.name)
        sub_dir = blob.name[len(gcs_path) + 1 : -len(file_name)]
        # Determine local_dir based on what source dir it belongs to
        local_dir = ""
        for source_dir in local_dir_map.keys():
            local_target = local_dir_map[source_dir]
            if Path("/" + sub_dir).is_relative_to(source_dir):
                if local_target is not None and local_target and local_target.strip():
                    local_dir = local_target
                    # Clean up sub_dir to no longer be an abs path so it will join properly for the output
                    if source_dir.startswith("/"):
                        source_dir = source_dir[1:]
                    if source_dir.endswith("/"):
                        source_dir = source_dir[:-1]
                    sub_dir = sub_dir[len(source_dir) + 1 :]
                else:
                    local_dir = None
                break
        if local_dir is None:
            logging.info(
                'Skipping downloading "%s" because no output directory was selected.',
                file_name,
            )
            continue
        if not local_dir:
            # The results files which should be output to the first valid output directory
            for target_dir in local_dir_map.values():
                if target_dir is not None and target_dir and target_dir.strip():
                    local_dir = target_dir
                    break
        file_dir = join(local_dir, sub_dir)
        os.makedirs(file_dir, exist_ok=True)
        file_path = join(file_dir, file_name)
        logging.info('Downloading output file to "%s"...', file_path)
        blob.download_to_filename(file_path)

    logging.info('Finished downloading. Output files are in "%s".', local_dir_map)
