#!/bin/bash -l

JOB_HOME="$(cd "$(dirname "$0")" && pwd)"
PYLINTRC=$JOB_HOME/.pylintrc

echo ">>>>> code formatting"
find . -type f -name "*.py" -exec black {} +

echo ">>>>> static analysis with pylint"
if [ ! -f "$PYLINTRC" ]; then
    echo ">> generate .pylintrc"
    pylint --generate-rcfile >"$PYLINTRC"
fi

echo ">>>>> code analysis"
find . -type f -name "*.py" -exec pylint --output-format=colorized {} +
