#!/bin/bash

if [ $TRAVIS_OS_NAME = 'windows' ]; then
  export JAVA_HOME=${JAVA_HOME:-/c/jdk}
  export PATH=${JAVA_HOME}/bin:${PATH}
  choco install openjdk11 -params 'installdir=c:\\jdk' -y
fi