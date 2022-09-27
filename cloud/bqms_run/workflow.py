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
import logging
from collections.abc import Callable
from concurrent.futures import Executor, Future, as_completed
from threading import Lock
from typing import Optional, ParamSpec, Type, TypeVar

from google.cloud.bigquery_migration_v2 import CreateMigrationWorkflowRequest

from bqms_run.gcp.bqms.request import execute as execute_bqms_request
from bqms_run.macro_processor import MacroProcessor
from bqms_run.paths import Path, Paths

logger = logging.getLogger(__name__)


_P = ParamSpec("_P")
_T = TypeVar("_T")


class SynchronousExecutor(Executor):
    """Synchronous executor."""

    def __init__(self) -> None:
        self._shutdown = False
        self._shutdownLock = Lock()  # pylint: disable=invalid-name

    def submit(  # pylint: disable=arguments-differ
        self, __fn: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs
    ) -> Future[_T]:
        with self._shutdownLock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")

            future: Future[_T] = Future()
            try:
                result = __fn(*args, **kwargs)
            except Exception as exc:  # pylint: disable=broad-except
                future.set_exception(exc)
            else:
                future.set_result(result)

            return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        with self._shutdownLock:
            self._shutdown = True


ProcessingHook = Callable[[Path, str], str]


def execute(
    paths: Paths,
    preprocess_hook: ProcessingHook,
    postprocess_hook: ProcessingHook,
    bqms_request: CreateMigrationWorkflowRequest,
    macro_processor: Optional[MacroProcessor] = None,
    executor_factory: Type[Executor] = SynchronousExecutor,
) -> None:
    """Executes the preprocess->translate->postprocess workflow.

    Example:
        .. code-block::

            from bqms_run.gcp.bqms.request import build as build_bqms_request
            from bqms_run.hooks import (
                postprocess as postprocess_hook,
                preprocess as preprocess_hook
            )
            from bqms_run.macro_processor import MacroProcessor
            from bqms_run.paths import Paths

            paths = Paths.from_mapping(...)
            bqms_request = build_bqms_request(...)
            macro_processor = MacroProcessor.from_mapping(...)
            execute(paths, preprocess_hook, postprocess_hook, bqms_request,
                    macro_processor)

    Args:
        paths: A run.paths.Paths instance containing project paths.
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
        executor_factory: An optional concurrent.futures.Executor factory used
            to create an executor for per-path pre and postprocessing tasks.
    """

    def _preprocess(source_file_path: Path) -> None:
        """Preprocessing task.

        To be submitted to executor for each input path.
        """
        logger.debug("Preprocessing: %s.", source_file_path)
        relative_file_path = source_file_path.relative_to(paths.input_path)
        target_file_path = paths.preprocessed_path / relative_file_path
        target_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Do not preprocess metadata zip file.
        if relative_file_path.name.endswith(".zip"):
            with source_file_path.open(mode="rb") as source_file:
                source_bytes = source_file.read()
            with target_file_path.open(mode="wb") as target_file:
                target_file.write(source_bytes)
            return

        with source_file_path.open(mode="r", encoding="utf-8") as source_file:
            source_text = source_file.read()

        preprocessed_text = preprocess_hook(relative_file_path, source_text)
        macro_expanded_text = (
            macro_processor.expand(relative_file_path, preprocessed_text)
            if macro_processor
            else preprocessed_text
        )

        with target_file_path.open(mode="w", encoding="utf-8") as target_file:
            target_file.write(macro_expanded_text)

    def _postprocess(source_file_path: Path) -> None:
        """Postprocessing task.

        To be submitted to executor for each translated path.
        """
        logger.debug("Postprocessing: %s.", source_file_path)
        relative_file_path = source_file_path.relative_to(paths.translated_path)
        target_file_path = paths.postprocessed_path / relative_file_path
        target_file_path.parent.mkdir(parents=True, exist_ok=True)

        with source_file_path.open(mode="r", encoding="utf-8") as source_file:
            source_text = source_file.read()

        macro_unexpanded_text = (
            macro_processor.unexpand(relative_file_path, source_text)
            if macro_processor
            else source_text
        )
        postprocessed_text = postprocess_hook(relative_file_path, macro_unexpanded_text)

        with target_file_path.open(mode="w", encoding="utf-8") as target_file:
            target_file.write(postprocessed_text)

    with executor_factory() as executor:
        futures = []

        # Preprocess.
        logger.info("Preprocessing input paths.")
        for input_path in paths.input_path.rglob("*"):
            if input_path.is_file():
                futures.append(executor.submit(_preprocess, input_path))

        # Trigger any exceptions caught during preprocessing.
        for future in as_completed(futures):
            future.result()
        futures.clear()

        # Execute translation.
        execute_bqms_request(bqms_request)

        # Postprocess.
        logger.info("Postprocessing translated paths.")
        for translated_path in paths.translated_path.rglob(
            "*"
        ):  # type: ignore[no-untyped-call]
            if translated_path.is_file():
                futures.append(executor.submit(_postprocess, translated_path))

        # Trigger any exceptions caught during postprocessing.
        for future in as_completed(futures):
            future.result()
