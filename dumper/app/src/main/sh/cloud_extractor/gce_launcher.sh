#!/bin/bash

set -e
set -o pipefail

CLOUD_LOG='launcher'
dwh_url_default='https://github.com/google/dwh-migration-tools/releases/download/v@dumper_version@/dwh-migration-tools-v@dumper_version@.zip'

get_metadata() {
  curl -sf "http://metadata.google.internal/computeMetadata/v1/instance/$1" -H "Metadata-Flavor: Google" || true
}

get_attribute() {
  local attr_override_ref="DWH_ATTRIBUTE_OVERRIDE_$1"
  local attr_override="${!attr_override_ref}"
  if [[ -z "${attr_override}" ]]; then
    get_metadata "attributes/$1"
  else
    echo "${attr_override}"
  fi
}

log() {
  echo "$1 - $2" > /dev/stderr
  gcloud logging write "${CLOUD_LOG}" "$2" --severity="$1" || true
}

die() {
  log ERROR "$1"
  exit 1
}

stop() {
  case "$(get_attribute dwh_onfinish)" in
  delete)
    log INFO 'Deleting instance'
    gcloud --quiet compute instances delete "$(get_metadata name)" --zone="$(get_metadata zone)"
    ;;
  stop)
    log INFO 'Stopping instance'
    gcloud --quiet compute instances stop "$(get_metadata name)" --zone="$(get_metadata zone)"
    ;;
  suspend)
    log INFO 'Pausing instance'
    gcloud --quiet compute instances suspend "$(get_metadata name)" --zone="$(get_metadata zone)"
    ;;
  *)
    log INFO 'Keeping instance.'
    ;;
  esac
}

trap 'stop' EXIT

install_dependencies() {
  if which apt-get > /dev/null; then
    log INFO 'Installing dependencies if necessary using apt-get.'
    which unzip > /dev/null || sudo apt-get install -y -q unzip
    which java > /dev/null || sudo apt-get install -y -q default-jre
  else
    log WARN 'Could not find apt-get. Skipping installing dependencies.'
  fi
}

download() {
  local url="$1"
  local target="$2"
  log INFO "Downloading '${url}' to '${target}."
  if [[ ! -e "${target}" ]]; then
    if [[ "${url}" = gs://* ]]; then
      gsutil cp "${url}" "${target}"
    else
      curl -L "${url}" -o "${target}"
    fi
  fi
}

validate_program() {
  local program="$1"
  if ! which "${program}" > /dev/null; then
    echo "Could not find ${program} in PATH."
    return 1
  fi
}

validate_metadata_access() {
  if ! curl 'http://metadata.google.internal/' > /dev/null 2> /dev/null; then
    echo "Cannot access http://metadata.google.internal/. Are we running on a GCE instance?" > /dev/stderr
  fi
}


validate_instance() {
  validate_program gcloud && validate_program curl && validate_metadata_access || return 1
}

main() {
  install_dependencies
  dwh_tmp="${DWH_TMP:-$(mktemp -d "${TMPDIR}/dwh.XXX")}"
  dwh_zip="${dwh_tmp}/dwh-migration-tools.zip"

  dwh_url="$(get_attribute dwh_download_url)"

  log INFO 'Downloading the dumper.'
  download "${dwh_url:-${dwh_url_default}}" "${dwh_zip}"

  unzip -o "${dwh_zip}" -d "${dwh_tmp}"
  dwh_home="$(find "${dwh_tmp}" -maxdepth 1 -name 'dwh-migration-tools' -o -name 'dwh-migration-tools-*')"
  if [[ -z "${dwh_home}" ]]; then
    die "ZIP file did not contain dwh-migration-tools."
  fi

  dwh_cloud_extractor="${dwh_home}/bin/dwh-cloud-extractor"
  if [[ ! -x "${dwh_cloud_extractor}" ]]; then
    die "Could not find cloud extractor bin. Got no executable '${dwh_cloud_extractor}'."
  fi

  log INFO 'Launching the dumper.'
  "${dwh_cloud_extractor}"
  log INFO "Dumper exited with code $?."
}


export TMPDIR="${TMPDIR:-/tmp}"

if ! validate_instance; then
  die 'This script is meant to be run on a GCE instance as a launch script.'
fi
log INFO 'Preparing cloud extractor.'

main
