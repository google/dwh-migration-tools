#!/bin/bash

set -o pipefail

# Removes credentials from JDBC URLs: the values of URL parameters, user information before the
# host, and Oracle "user/password@". Database type, host, port and database name are kept.
redact_jdbc_urls() {
  sed -E \
    -e '/jdbc:/s#([?&;,(][^=?&;,()| ]+=)[^&;,()| ]*#\1<redacted>#g' \
    -e '/jdbc:/s#(://)[^/?| ]*@#\1#g' \
    -e '/jdbc:/s#(jdbc:oracle:[A-Za-z0-9]+:)[^@?;(),| ][^?;(),| ]*@#\1@#g' \
    -e '/jdbc:/s#(jdbc:oracle:[A-Za-z0-9]+:)[^@| ][^| ]*@#\1@#g'
}

if command -v beeline > /dev/null 2>&1 ; then
  # Both stdout and stderr are redacted; stderr can contain the URL in error messages.
  { beeline -u jdbc:hive2:// -e "set javax.jdo.option.ConnectionURL" 2>&1 1>&3 3>&- \
      | redact_jdbc_urls 1>&2 3>&- ; } 3>&1 \
    | redact_jdbc_urls
fi
