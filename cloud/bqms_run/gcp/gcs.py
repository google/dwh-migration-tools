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

import threading

import cloudpathlib
from google.cloud.storage.client import Client


class GSClient(cloudpathlib.GSClient):
    """Thread-safe cloudpathlib client for GCS."""

    def __init__(self, project: str):
        self._project = project
        super().__init__(project=project)

    _thread_local = threading.local()

    @property
    def client(self) -> Client:
        if getattr(self._thread_local, "client", None) is None:
            self._thread_local.client = Client(project=self._project)
        return self._thread_local.client

    @client.setter
    def client(self, value: Client) -> None:
        """No-op. GSClient.client is read-only."""
