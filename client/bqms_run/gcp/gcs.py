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
"""Thread-safe cloudpathlib client for GCS."""
import os
import threading
from pathlib import Path
from typing import IO, Any, Optional, Union

import cloudpathlib
from google.cloud.storage import Client, constants
from typing_extensions import ParamSpec

_P = ParamSpec("_P")

# TODO: Enhance cloudpathlib to accept a timeout.
constants._DEFAULT_TIMEOUT = 240.0  # pylint: disable=protected-access


class GSClient(cloudpathlib.GSClient):
    """Thread-safe cloudpathlib client for GCS."""

    def __init__(self, project: str) -> None:
        self._project = project
        super().__init__(project=project)

    _thread_local = threading.local()

    @property
    def client(self) -> Client:
        if getattr(self._thread_local, "client", None) is None:
            self._thread_local.client = Client(project=self._project)
        _client: Client = self._thread_local.client
        return _client

    @client.setter
    def client(self, value: Client) -> None:
        """No-op. GSClient.client is read-only."""

    def set_as_default_client(self) -> None:
        """Set this client instance as the default one used when instantiating
        cloud path instances for this cloud without a client specified."""
        cloudpathlib.GSClient._default_client = self  # type: ignore[misc] # pylint: disable=protected-access

    def _download_file(
        self,
        cloud_path: cloudpathlib.GSPath,
        # Subscript type arg not supported on ABCs in Python < 3.9.
        local_path: Union[str, os.PathLike],  # type: ignore[type-arg]
    ) -> Path:
        """Override to use bucket.blob instead of bucket.get_blob.

        bucket.get_blob incurs a network request that is unnecessary for our use
        case.
        """
        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.blob(cloud_path.blob)

        local_path = Path(local_path)

        blob.download_to_filename(local_path)
        return local_path


class GSPath(cloudpathlib.GSPath):
    """GSPath optimized for this tool.

    The default implementation of GSPath provided by cloudpathlib does a lot of
    checks to ensure locally cached paths are in sync with cloud paths, paths
    passed to `open` are indeed files, etc. The main reason it performs these
    checks is it assumes that cloud paths are likely to be manipulated out of
    band. For most, if not all, of our users this assumption will be false. The
    added network requests from these checks can increase execution times by
    2-3x, so we provide this optimized variant of GSPath which eschews most of
    these checks.
    """

    client: GSClient

    def download_to(
        self,
        # Subscript type arg not supported on ABCs in Python < 3.9.
        destination: Union[str, os.PathLike],  # type: ignore[type-arg]
    ) -> Path:
        """Download GSPath to local cache."""
        destination = Path(destination)
        if destination.is_dir():
            destination = destination / self.name
        return self.client._download_file(  # pylint: disable=protected-access
            self, destination
        )

    def _upload_local_to_cloud(
        self, force_overwrite_to_cloud: bool = False
    ) -> "GSPath":
        """Uploads cache file at self._local to the cloud"""
        # We should never try to be syncing entire directories; we should only
        # cache and upload individual files.
        if self._local.is_dir():
            raise ValueError("Only individual files can be uploaded to the cloud")

        self.client._upload_file(self._local, self)  # pylint: disable=protected-access

        # reset dirty and handle now that this is uploaded
        self._dirty = False
        self._handle = None

        return self

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        force_overwrite_from_cloud: bool = False,  # extra kwarg not in pathlib
        force_overwrite_to_cloud: bool = False,  # extra kwarg not in pathlib
    ) -> IO[Any]:
        """Open GSPath."""
        self._local.parent.mkdir(parents=True, exist_ok=True)

        is_write = any(m in mode for m in ("w", "+", "x", "a"))

        if not is_write:
            self.download_to(self._local)

        if self._local.exists():
            original_mtime = self._local.stat().st_mtime
        else:
            original_mtime = 0

        buffer: IO[Any] = self._local.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

        # write modes need special on closing the buffer
        if is_write:
            # dirty, handle, patch close
            original_close = buffer.close

            # since we are pretending this is a cloud file, upload it to the
            # cloud when the buffer is closed
            def _patched_close(*args: _P.args, **kwargs: _P.kwargs) -> None:
                original_close(*args, **kwargs)

                # original mtime should match what was in the cloud; because of
                # system clocks or rounding by the cloud provider, the new
                # version in our cache is "older" than the original version;
                # explicitly set the new modified time to be after the original
                # modified time.
                if self._local.stat().st_mtime < original_mtime:
                    new_mtime = original_mtime + 1
                    os.utime(self._local, times=(new_mtime, new_mtime))

                self._upload_local_to_cloud(
                    force_overwrite_to_cloud=force_overwrite_to_cloud
                )

            buffer.close = _patched_close  # type: ignore[assignment]

            # keep reference in case we need to close when __del__ is called on
            # this object
            self._handle = buffer  # type: ignore[assignment]

            # opened for write, so mark dirty
            self._dirty = True

        return buffer
