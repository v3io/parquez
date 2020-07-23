#!/usr/bin/env bash

SOURCE_PATH='/home/iguazio/parquez'

if ! [ -d venv  ] ; then
        mkdir venv
        virtualenv venv
        source venv/bin/activate
        pip install -r config/requirements.txt
else
        source venv/bin/activate
fi


echo alter_kv_view "${1}" "${2}"

python core/delete_partition.py "${1}" "${2}"

