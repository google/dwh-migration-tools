#!/bin/bash

if command -v grafana-server > /dev/null 2>&1 ; then
  grafana-server -v
fi
