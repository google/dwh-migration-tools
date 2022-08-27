# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Preprocess->translate->postprocess workflow."""

import pathlib
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from google.cloud.bigquery_migration_v2 import CreateMigrationWorkflowRequest
from google.cloud.storage.blob import Blob

from run.gcp.bqms.request import execute as execute_bqms_request
from run.gcp.gcs import GCS
from run.macro_processor import MacroProcessor

ProcessingHook = Callable[[pathlib.Path, str], str]


def execute(
    gcs: GCS,
    preprocess_hook: ProcessingHook,
    postprocess_hook: ProcessingHook,
    bqms_request: CreateMigrationWorkflowRequest,
    macro_processor: Optional[MacroProcessor] = None,
) -> None:
    """Executes the preprocess->translate->postprocess workflow.

    Example:
        .. code-block::

            from run.gcp.bqms.request import build as build_bqms_request
            from run.gcp.gcs import GCS
            from run.hooks import (
                postprocess as postprocess_hook,
                preprocess as preprocess_hook
            )
            from run.macro_processor import MacroProcessor

            gcs = GCS.from_mapping(...)
            bqms_request = build_bqms_request(...)
            macro_processor = MacroProcessor.from_mapping(...)
            execute(gcs, preprocess_hook, postprocess_hook, bqms_request,
                    macro_processor)

    Args:
        gcs: A run.gcp.gcs.GCS instance for reading input and writing output.
        preprocess_hook: A callable for hooking user-defined preprocessing
            logic into the translation workflow.
        postprocess_hook: A callable for hooking user-defined postprocessing
            logic into the translation workflow.
        bqms_request: A
            google.cloud.bigquery_migration_v2.CreateMigrationWorkflowRequest
            for requesting the execution of the BQMS migration workflow.
        macro_processor: An optional run.macro_processor.MacroProcessor for
            handling macro/templating languages embedded in code to be
            translated.
    """

    def _preprocess(source_blob: Blob) -> None:
        """Preprocessing logic.

        To be submitted to thread pool executor for each input GCS blob.
        """
        relative_path = pathlib.Path(source_blob.name).relative_to(gcs.input_path)
        target_blob = gcs.bucket.blob(
            (gcs.preprocessed_path / relative_path).as_posix()
        )

        if relative_path.name.endswith(".zip"):
            target_blob.rewrite(source_blob)
            return

        input_text = source_blob.download_as_text()
        preprocessed_text = preprocess_hook(relative_path, input_text)
        macro_expanded_text = (
            macro_processor.expand(relative_path, preprocessed_text)
            if macro_processor
            else preprocessed_text
        )
        target_blob.upload_from_string(macro_expanded_text)

    def _postprocess(source_blob: Blob) -> None:
        """Postprocessing logic.

        To be submitted to thread pool executor for each translated GCS blob.
        """
        relative_path = pathlib.Path(source_blob.name).relative_to(gcs.translated_path)
        target_blob = gcs.bucket.blob(
            (gcs.postprocessed_path / relative_path).as_posix()
        )

        translated_text = source_blob.download_as_text()
        macro_unexpanded_text = (
            macro_processor.unexpand(relative_path, translated_text)
            if macro_processor
            else translated_text
        )
        postprocessed_text = postprocess_hook(relative_path, macro_unexpanded_text)
        target_blob.upload_from_string(postprocessed_text)

    with ThreadPoolExecutor() as executor:
        futures = []

        # Preprocess.
        for input_blob in gcs.bucket.list_blobs(prefix=gcs.input_path.as_posix()):
            futures.append(executor.submit(_preprocess, input_blob))

        # Trigger any exceptions caught during preprocessing.
        for future in as_completed(futures):
            future.result()
        futures.clear()

        # Execute translation.
        execute_bqms_request(bqms_request)

        # Postprocess.
        for translated_blob in gcs.bucket.list_blobs(
            prefix=gcs.translated_path.as_posix()
        ):
            futures.append(executor.submit(_postprocess, translated_blob))

        # Trigger any exceptions caught during postprocessing.
        for future in as_completed(futures):
            future.result()
