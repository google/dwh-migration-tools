#!/bin/bash

set -e

STEP_FORMAT=false
STEP_TEST=false
STEP_BUILD=false

if [[ $# -eq 0 ]] ; then
  STEP_FORMAT=true
  STEP_TEST=true
  STEP_BUILD=true
else
  while [[ $# -gt 0 ]]; do
    case $1 in
      -f)
        STEP_FORMAT=true
        shift
        ;;
      -t)
        STEP_TEST=true
        shift
        ;;
      -b)
        STEP_BUILD=true
        shift
        ;;
      -h)
        echo "Command to perform precommit actions."
        echo
        echo "Usage: precommit.sh [-f] [-t] [-b]"
        echo
        echo "If no option is specified, then all actions are executed."
        echo
        echo "Options:"
        echo "  -f format the files"
        echo "  -t run unit tests"
        echo "  -b build the dist"
        exit
        ;;
      *)
        echo "ERROR: Unknown option '$1'."
        exit 1
        ;;
    esac
  done
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GRADLE_CMD="$SCRIPT_DIR/gradlew"

if [[ "$STEP_FORMAT" == "true" ]] ; then
  "$GRADLE_CMD" spotlessApply
fi

if [[ "$STEP_TEST" == "true" ]] ; then
  "$GRADLE_CMD" :dumper:app:test
  "$GRADLE_CMD" :permissions-migration:app:test
fi

if [[ "$STEP_BUILD" == "true" ]] ; then
  "$GRADLE_CMD" --parallel :dumper:app:installDist
  "$GRADLE_CMD" --parallel :permissions-migration:app:installDist
fi
