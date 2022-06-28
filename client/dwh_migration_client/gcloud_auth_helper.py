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
"""A simple helper class to check the status of GCP login and authentication through the
gcloud CLI."""

import logging
import os
import subprocess


def validate_gcloud_auth_settings(project_number: str) -> None:
    """Validates the gcloud login and authentication settings for the user through
    gcloud CLI command. If validation fails, the method will try to log in and generate
    an application-default-authentication.json file.

    Args:
        project_number: the GCP project number.
    """
    gcloud_auth_helper = GcloudAuthHelper(project_number)
    gcloud_auth_helper.validate_login_status()
    gcloud_auth_helper.validate_auth_status()
    gcloud_auth_helper.validate_project_config()


class GcloudAuthHelper:
    """A simple helper class to check the status of GCP login and authentication through
    the gcloud CLI."""

    def __init__(self, project_number: str) -> None:
        self.project_number = project_number
        self.project_id = None
        self.user_email = None

    __GCLOUD_CRED_FILE = "~/.config/gcloud/application_default_credentials.json"
    __APPLICATION_DEFAULT_LOGIN_COMMAND = "gcloud auth application-default login"
    __AUTH_LIST = "gcloud auth list"
    __AUTH_LOGIN = "gcloud auth login"
    __CONFIG_LIST = "gcloud config list"
    __SET_PROJECT = "gcloud config set project"

    def validate_login_status(self) -> None:
        """Validates the gcloud login status for the user through gcloud CLI command.
        If validation fails, the method will try to log in through the gcloud auth login
        command.
        """
        logging.info("Validate user login status in gcloud...")
        result = subprocess.getoutput(self.__AUTH_LIST)
        if "No credentialed accounts" in result:
            logging.info(
                "User hasn't logged in to gcloud yet.  Running command to login "
                '"%s"...',
                self.__AUTH_LOGIN,
            )
            logging.info(
                "Please open the following link in a browser and grant permission to "
                "login..."
            )
            os.system(self.__AUTH_LOGIN)

    def validate_auth_status(self) -> None:
        """Validates the user credential status.
        If validation fails, the method will try to generate an application-default
        credential file.
        """
        logging.info("Validate user credential status in gcloud...")
        if not os.path.exists(os.path.expanduser(self.__GCLOUD_CRED_FILE)):
            logging.info(
                "Can't find application_default_credential file. Generating "
                'credential through the command "%s"',
                self.__APPLICATION_DEFAULT_LOGIN_COMMAND,
            )
            logging.info(
                "Please open the following link in a browser and grant permission to "
                "download credential file..."
            )
            os.system(self.__APPLICATION_DEFAULT_LOGIN_COMMAND)

    def validate_project_config(self) -> None:
        logging.info("Validate project config status in gcloud...")
        os.system("%s %s" % (self.__SET_PROJECT, self.project_number))
        result = subprocess.getoutput(self.__CONFIG_LIST)
        logging.info("Your cloud config used for this translation job is:\n%s", result)
        assert "account =" in result, (
            'Can\'t find account info in gcloud config. Please log in through "%s"'
            % self.__AUTH_LOGIN
        )
        assert (
            "project = %s" % self.project_number in result
        ), "Can't find GCP project number in gcloud config."
