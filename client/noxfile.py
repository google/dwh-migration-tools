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
"""Dev commands."""
import pathlib
import tempfile

import nox


@nox.session(tags=["analyze"], python=False)
def format(session):  # pylint: disable=redefined-builtin
    """Execute addlicense, black and isort."""
    session.run("poetry", "install")
    session.run("addlicense", "examples", "bqms_run", "tests")
    session.run("poetry", "run", "black", "bqms_run", "tests", "noxfile.py")
    session.run("poetry", "run", "isort", "bqms_run", "tests", "noxfile.py")


@nox.session(tags=["analyze"], python=False)
def lint(session):
    """Execute linters."""
    session.run("poetry", "install")
    session.run("npm", "install")
    test_paths = set(p.as_posix() for p in pathlib.Path("tests").rglob("*.py"))
    session.run("poetry", "run", "pylint", "bqms_run", "noxfile.py", *test_paths)
    session.run("poetry", "run", "yamllint", "examples")
    session.run("npm", "exec", "markdownlint-cli2", "*.md")
    for json_path in set(
        p.as_posix() for p in pathlib.Path("examples").rglob("*.json")
    ):
        session.run("npm", "exec", "--", "jsonlint", "--in-place", "--quiet", json_path)


@nox.session(tags=["analyze"], python=False)
def type_check(session):
    """Execute mypy."""
    session.run("poetry", "install")
    session.run("poetry", "run", "mypy", "--strict", "bqms_run")


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def tests(session):
    """Execute tests."""
    with tempfile.NamedTemporaryFile() as temp_file:
        session.install("poetry")
        session.run(
            "poetry",
            "export",
            "--with",
            "dev",
            "--without-hashes",
            "--output",
            temp_file.name,
        )
        session.install("-r", temp_file.name)
        session.install(".")
        tests_path = pathlib.Path("tests")
        if "unit" in session.posargs:
            tests_path = tests_path / "unit"
        if "integration" in session.posargs:
            tests_path = tests_path / "integration"
        if "pytest_verbose" in session.posargs:
            session.run(
                "coverage",
                "run",
                "--source=bqms_run",
                "-m",
                "pytest",
                "-vv",
                "--log-cli-level=INFO",
                "--log-cli-format="
                "%(asctime)s: %(levelname)s: %(threadName)s: "
                "%(filename)s:%(lineno)s: %(message)s",
                tests_path.as_posix(),
            )
        else:
            session.run(
                "coverage",
                "run",
                "--source=bqms_run",
                "-m",
                "pytest",
                tests_path.as_posix(),
            )
        session.run("coverage", "report", "-m")
