# The tool builds a terraform configuration file based on input permissions

## Requirements
- Python 3.7 or higher
- Setup virtual environment: `python -m venv venv`
- Activate virtual environment: `source venv/bin/activate`
- Install libs: `pip install -r requirements.txt`

If there is an error during requirements installation following steps should help:
```
pip install "cython<3.0.0" wheel
pip install "pyyaml==5.4.1" --no-build-isolation
pip install -r requirements.txt
```

## Generate terraform template
### From local environment
1. Setup requirements mentioned above.
2. Add `permissions.yaml` file into the `data` directory.
3. Run the command: `python tf_generator data/permissions.yaml <storage location> <output directory>`

### From Dockerfile
1. Add `permissions.yaml` file into the `data` directory.
2. Setup [docker](https://www.docker.com/).
3. Build the image: `docker  build -t tf-template-builder .`
4. Run the container: `docker run -it --rm --mount type=bind,source=<path-to-terraform-folder>/terraform/out,target=/app/out tf-template-builder bash`
5. Copy generated GCS permissions into the `/app/out/` folder.

## How to run unit test
1. Setup and activate virtual environment from requirements.
2. Run `python -m unittest`. Or `python -m unittest -v` for more verbosity.


## Test generated terraform template
1. Setup [docker](https://www.docker.com/).
2. Go to the folder: `test/test_generated_tf_template`
3. Insert the template content into the `example.tf` file.
3. Build the image: `docker build -t tf-permission-builder .`
4. Run the container: `docker run -it --rm -v gcloud-cred:/root/.config/gcloud tf-permission-builder bash`
5. Login to GCP account (inside the container): `gcloud auth application-default login`
5. Now it's possible to run terraform commands (for ex: `terraform apply`).
