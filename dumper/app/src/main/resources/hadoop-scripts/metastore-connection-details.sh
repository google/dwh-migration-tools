#!/bin/bash

set -o pipefail

# Removes credentials from JDBC URLs: the values of URL parameters, user information before the
# host, and Oracle "user/password@". Database type, host, port and database name are kept.
#
# The rules run in this order:
# 1. MySQL, MariaDB and PostgreSQL URLs: user information before the host. It runs first, so that
#    user information is not taken for a parameter.
# 2. Values in ";key=value" lists. A value ends at the next ";". A value in braces ends at its
#    closing brace, and "}}" inside it is part of the value.
# 3. Values in "?key=value&key=value" lists. A value ends at the next "&".
# 4. Values of "key=value" pairs that start with "," or "(". A value ends at the next "," or
#    parenthesis.
# 5. User information before the host, for all URLs.
# 6. Oracle "user/password@".
redact_jdbc_urls() {
  sed -E \
    -e '/jdbc:/s#(jdbc:(mysql|mariadb|postgresql)[a-z+:]*://)[^/?()| ]*@#\1#g' \
    -e '/jdbc:/s#(;[^=?&;,()| ]+=)(\{([^}| ]|\}\})*\}|[^;| ]*)#\1<redacted>#g' \
    -e '/jdbc:/s#([?&][^=?&;,()| ]+=)[^&| ]*#\1<redacted>#g' \
    -e '/jdbc:/s#([,(][^=?&;,()| ]+=)[^,()| ]*#\1<redacted>#g' \
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
