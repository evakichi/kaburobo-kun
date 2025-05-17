#!/bin/bash

SCRIPT_DIR=$(cd $(dirname $0);pwd)

cd ${SCRIPT_DIR}

if ! . ../.env; then
    echo ".env error"
    exit 10;
fi

python3 ../py/data_obtainer.py ${1}

