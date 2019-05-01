from utils.logger import Logger
from config.app_conf import AppConf
from core.kv_table import KVTable


# test_config.py


def test_generate():
    logger = Logger()
    cf = AppConf(logger,config_path='../config/parquez.ini')
    kvtable = KVTable('parquez', 'booking_service_kv',logger)
    assert kvtable.container_name == "parquez"
    assert kvtable.name == 'booking_service_kv'
    assert kvtable.schema == '{"fields":[{"name":"creation_time","type":"string","nullable":true},{"name":"booking_code","type":"string","nullable":true},{"name":"metadata","type":"string","nullable":true},{"name":"driver_id","type":"long","nullable":true},{"name":"distance","type":"double","nullable":true},{"name":"cashless_status","type":"long","nullable":true},{"name":"promotion_id","type":"long","nullable":true},{"name":"source","type":"string","nullable":true},{"name":"type","type":"string","nullable":true},{"name":"customer_complete","type":"long","nullable":true},{"name":"is_peak","type":"long","nullable":true},{"name":"requested_promotion_id","type":"long","nullable":true},{"name":"passenger2_id","type":"long","nullable":true},{"name":"passenger1_name","type":"string","nullable":true},{"name":"tip","type":"double","nullable":true},{"name":"state","type":"string","nullable":true},{"name":"id","type":"long","nullable":true},{"name":"fare_upper_bound","type":"double","nullable":true},{"name":"value","type":"string","nullable":true},{"name":"attempts","type":"long","nullable":true},{"name":"last_updated","type":"string","nullable":true},{"name":"vehicle_type_name","type":"string","nullable":true},{"name":"possible_promotion","type":"long","nullable":true},{"name":"distance_source","type":"string","nullable":true},{"name":"won_by_fleet_id","type":"long","nullable":true},{"name":"close_time","type":"string","nullable":true},{"name":"passenger1_phone","type":"string","nullable":true},{"name":"surface_time","type":"string","nullable":true},{"name":"vehicle_type_id","type":"long","nullable":true},{"name":"passenger1_id","type":"long","nullable":true},{"name":"payment_token_id","type":"string","nullable":true},{"name":"user_id","type":"long","nullable":true},{"name":"fare_lower_bound","type":"double","nullable":true},{"name":"created_by_fleet_id","type":"long","nullable":true},{"name":"passenger3_id","type":"long","nullable":true},{"name":"pickup_time","type":"string","nullable":true},{"name":"remarks","type":"string","nullable":true},{"name":"city_id","type":"long","nullable":true},{"name":"year","type":"long","nullable":true},{"name":"month","type":"long","nullable":true},{"name":"day","type":"long","nullable":true},{"name":"hour","type":"long","nullable":true},{"name":"minute","type":"long","nullable":true}]}\n'
    assert kvtable.get_schema_fields_and_types() == """creation_time string,
booking_code string,
metadata string,
driver_id long,
distance double,
cashless_status long,
promotion_id long,
source string,
type string,
customer_complete long,
is_peak long,
requested_promotion_id long,
passenger2_id long,
passenger1_name string,
tip double,
state string,
id long,
fare_upper_bound double,
value string,
attempts long,
last_updated string,
vehicle_type_name string,
possible_promotion long,
distance_source string,
won_by_fleet_id long,
close_time string,
passenger1_phone string,
surface_time string,
vehicle_type_id long,
passenger1_id long,
payment_token_id string,
user_id long,
fare_lower_bound double,
created_by_fleet_id long,
passenger3_id long,
pickup_time string,
remarks string,
city_id long,
year long,
month long,
day long,
hour long,
minute long"""
    assert kvtable.get_schema_fields() == """  creation_time,
  booking_code,
  metadata,
  driver_id,
  distance,
  cashless_status,
  promotion_id,
  source,
  type,
  customer_complete,
  is_peak,
  requested_promotion_id,
  passenger2_id,
  passenger1_name,
  tip,
  state,
  id,
  fare_upper_bound,
  value,
  attempts,
  last_updated,
  vehicle_type_name,
  possible_promotion,
  distance_source,
  won_by_fleet_id,
  close_time,
  passenger1_phone,
  surface_time,
  vehicle_type_id,
  passenger1_id,
  payment_token_id,
  user_id,
  fare_lower_bound,
  created_by_fleet_id,
  passenger3_id,
  pickup_time,
  remarks,
  city_id,
  minute,year,month,day,hour"""

def test_kv_table():
    logger = Logger()
    logger.info("Starting to Parquezzzzzzzz")

    conf = AppConf(logger, "test.ini")
    conf.log_conf()

    logger.info("validating kv table")
    kv_table = KVTable(conf, "booking_service_kv", logger)
    kv_table.import_table_schema()



