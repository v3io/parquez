#!/usr/bin/env bash

pushd ../core

python alter_kv_view.py $1 $2

popd