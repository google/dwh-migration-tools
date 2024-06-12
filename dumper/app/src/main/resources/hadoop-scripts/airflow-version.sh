#!/bin/bash

if command -v airflow > /dev/null 2>&1 ; then
  airflow version
fi