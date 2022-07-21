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
"""Execute commands against different Python versions."""
import nox


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def test(session):
    """Execute pytest."""
    session.install(".[dev]")
    if "pytest_verbose" in session.posargs:
        session.run(
            "pytest",
            "-vv",
            "--log-cli-level=INFO",
            "--log-cli-format="
            "%(asctime)s: %(levelname)s: %(filename)s:%(lineno)s: %(message)s",
        )
    else:
        session.run("pytest")
