#!/bin/bash

DEFAULT_PYTHON="/usr/local/google/home/binwu/tools/anaconda3/bin/python3"
if [ -z "${PYTHON:-}" ]; then
    if [ -x "$DEFAULT_PYTHON" ]; then
        PYTHON="$DEFAULT_PYTHON"
    else
        PYTHON="python3"
    fi
fi

exec make PYTHON="$PYTHON" "$@"
