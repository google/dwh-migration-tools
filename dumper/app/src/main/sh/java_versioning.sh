#!/bin/bash

function is_java_greater_than_8() {
  java_path=$(which java)
  java_version_output=$($java_path -version 2>&1)

  # Will return 1 for all versions before Java 9, but we do not care.
  java_major_version=$(echo "$java_version_output" | grep -Eoi 'version "?([0-9]+)' | head -1 | cut -d'"' -f2 | cut -d'.' -f1)

  # Check if the major version is greater than 8.
  if [[ $java_major_version -gt 8 ]]; then
    return 0  # True
  else
    return 1  # False
  fi
}