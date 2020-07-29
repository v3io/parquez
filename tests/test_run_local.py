from conftest import (
    examples_path, has_secrets, here, out_path, tag_test, verify_state
)
from mlrun import NewTask, run_local, code_to_function
from mlrun import NewTask, get_run_db, new_function

ARTIFACTS_PATH = ''

base_spec = NewTask(params={'view_name': 'view_name'
    , 'partition_by': 'h'
    , 'partition_interval': '1h'
    , 'real_time_window': '3h'
    , 'historical_retention': '21h'
    , 'real_time_table_name': 'faker'
    , 'config_path': 'test.ini'}, out_path=out_path)


def test_run_local_get_schema():
    spec = tag_test(base_spec, 'test_run_local_parquet')
    result = run_local(spec, command='../functions/get_table_schema.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)


def test_run_local_parquet():
    spec = tag_test(base_spec, 'test_run_local_parquet')
    result = run_local(spec, command='../functions/create_parquet_table.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)


def test_run_create_kv_view():
    spec = tag_test(base_spec, 'test_run_create_kv_view')
    result = run_local(spec, command='../functions/create_kv_view.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)


def test_run_create_unified_view():
    spec = tag_test(base_spec, 'test_run_create_unified_view')
    result = run_local(spec, command='../functions/create_unified_view.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)


def test_run_parquez_interval():
    spec = tag_test(base_spec, 'test_run_parquez_interval')
    result = run_local(spec, command='../functions/run_parquez_interval.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)


def test_run_scheduler():
    spec = tag_test(base_spec, 'test_run_scheduler')
    result = run_local(spec, command='../functions/run_scheduler.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)


def test_clean_parquez():
    spec = tag_test(base_spec, 'test_clean_parquez')
    result = run_local(spec, command='../functions/clean_parquez.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)
