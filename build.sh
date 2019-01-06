#!/bin/bash

check()
{
  if [ "$?" != "0" ]; then
    echo "**failed!" 1>&2
    exit 1
  fi
}

echo "building ..."
cd spark
sbt clean package
check
cd ..
