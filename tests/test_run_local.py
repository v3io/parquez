from conftest import (
    out_path, tag_test, verify_state
)
from mlrun import new_task, run_local

ARTIFACTS_PATH = ''

base_spec = new_task(params={'view_name': 'view_name',
                             'partition_by': 'h',
                             'partition_interval': '1h',
                             'real_time_window': '3h',
                             'historical_retention': '24h',
                             'real_time_table_name': 'faker',
                             'config_path': 'test.ini',
                             'user_name': 'avia',
                             'access_key': '2992c456-f293-42e4-9915-622bbab9bcd6',
                             'project_name': 'test-parquez',
                             'kv_path': 'v3io://parquez/faker/year=2021/month=02/day=08/hour=06/',
                             'parquet_path': 'v3io://parquez/faker_Parquet/year=2021/month=02/day=08/hour=06/',
                             'fuse_kv_path': '/v3io/parquez/faker/year=2021/month=02/day=08/hour=06/'}
                     , out_path=out_path)


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


def test_add_partition():
    spec = tag_test(base_spec, 'test_add_partition')
    result = run_local(spec, command='../functions/parquet_add_partition.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)


def test_parquetinizer():
    spec = tag_test(base_spec, 'test_parquetinizer')
    result = run_local(spec, command='../functions/parquetinizer.py', workdir='./', artifact_path='./artifacts')
    verify_state(result)