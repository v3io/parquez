#!/usr/bin/env bash
pip install requests
#if ! [ -d venv  ] ; then
#        mkdir venv
#        virtualenv venv
#        source venv/bin/activate
#        pip install -r config/requirements.txt
#else
#        source venv/bin/activate
#fi

parquez_dir='parquez'

command='python '${parquez_dir}'/core/alter_kv_view.py '"${1}"' '"${2}"

echo "${command}" 2>&1

eval "${command}" 2>&1

