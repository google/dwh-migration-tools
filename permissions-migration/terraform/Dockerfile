# The lowest Python version is expected in customer environemnt
FROM python:3.7

RUN mkdir -p app/tf_generator

COPY tf_generator /app/tf_generator
COPY data /app/data
COPY requirements.txt /app/
WORKDIR /app

RUN python -m venv venv
RUN venv/bin/pip install --require-hashes -r requirements.txt
RUN venv/bin/python tf_generator data/permissions.yaml EU
