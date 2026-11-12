#!/bin/bash

set -o pipefail

# Removes credentials from JDBC URLs: user information before the host, Oracle "user/password@",
# and the values of all URL parameters. Database type, host, port and database name are kept.
redact_jdbc_urls() {
  sed -E \
    -e '/jdbc:/s#//[^/?;| ]*@#//#' \
    -e '/jdbc:/s#(jdbc:oracle:[A-Za-z0-9]+:)[^| ]*@#\1@#' \
    -e '/jdbc:/s#([?&;,(][^=?&;,()| ]+=)[^&;,()| ]*#\1<redacted>#g'
}

if command -v beeline > /dev/null 2>&1 ; then
  # Both stdout and stderr are redacted; stderr can contain the URL in error messages.
  { beeline -u jdbc:hive2:// -e "set javax.jdo.option.ConnectionURL" 2>&1 1>&3 3>&- \
      | redact_jdbc_urls 1>&2 3>&- ; } 3>&1 \
    | redact_jdbc_urls
fi
