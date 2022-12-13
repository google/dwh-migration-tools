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

# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

ENV APP_USER bqms
ENV APP_UID 1000
ENV APP_GROUP bqms
ENV APP_GID 1000

RUN set -x \
    && apt-get update \
    && apt install -y sudo curl \
    && groupadd -r --gid $APP_GID $APP_GROUP \
    && useradd -r -m --gid $APP_GID --uid $APP_UID $APP_USER \
    && echo "$APP_USER ALL=(ALL)NOPASSWD:ALL" | EDITOR='tee -a' visudo

USER $APP_USER

RUN set -x \
    && curl -sSL https://install.python-poetry.org | python -

ENV PATH="/home/bqms/.local/bin:${PATH}"

COPY --chown=$APP_USER:$APP_GROUP . /opt/bqms

WORKDIR /opt/bqms

RUN set -x \
    && poetry install --no-interaction --no-ansi --only main

# https://cloud.google.com/run/docs/issues#home
# Cloud Run sets $HOME to /home. Poetry attempts to create the virtualenv in
# $HOME/.cache/pypoetry/virtualenvs which will not be writeable by $APP_USER
# if $HOME is set to /home. Thus, we ensure $HOME is properly set to
# /home/$APP_USER.
ENTRYPOINT HOME=/home/$APP_USER poetry run bqms-run
