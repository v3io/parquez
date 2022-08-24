# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from utils.logger import Logger
from config.app_conf import AppConf
from core.kv_table import KVTable


# test_kv_table.py
KVTABLE_NAME = "faker"
SCHEMA = b'{"fields":[{"name":"creation_time","type":"string","nullable":true},{"name":"booking_code","type":"string","nullable":true},{"name":"metadata","type":"string","nullable":true},{"name":"driver_id","type":"long","nullable":true},{"name":"distance","type":"double","nullable":true},{"name":"cashless_status","type":"long","nullable":true},{"name":"promotion_id","type":"long","nullable":true},{"name":"source","type":"string","nullable":true},{"name":"type","type":"string","nullable":true},{"name":"customer_complete","type":"long","nullable":true},{"name":"is_peak","type":"long","nullable":true},{"name":"requested_promotion_id","type":"long","nullable":true},{"name":"passenger2_id","type":"long","nullable":true},{"name":"passenger1_name","type":"string","nullable":true},{"name":"tip","type":"double","nullable":true},{"name":"state","type":"string","nullable":true},{"name":"id","type":"long","nullable":true},{"name":"fare_upper_bound","type":"double","nullable":true},{"name":"value","type":"string","nullable":true},{"name":"attempts","type":"long","nullable":true},{"name":"last_updated","type":"string","nullable":true},{"name":"vehicle_type_name","type":"string","nullable":true},{"name":"possible_promotion","type":"long","nullable":true},{"name":"distance_source","type":"string","nullable":true},{"name":"won_by_fleet_id","type":"long","nullable":true},{"name":"close_time","type":"string","nullable":true},{"name":"passenger1_phone","type":"string","nullable":true},{"name":"surface_time","type":"string","nullable":true},{"name":"vehicle_type_id","type":"long","nullable":true},{"name":"passenger1_id","type":"long","nullable":true},{"name":"payment_token_id","type":"string","nullable":true},{"name":"user_id","type":"long","nullable":true},{"name":"fare_lower_bound","type":"double","nullable":true},{"name":"created_by_fleet_id","type":"long","nullable":true},{"name":"passenger3_id","type":"long","nullable":true},{"name":"pickup_time","type":"string","nullable":true},{"name":"remarks","type":"string","nullable":true},{"name":"city_id","type":"long","nullable":true},{"name":"year","type":"long","nullable":true},{"name":"month","type":"long","nullable":true},{"name":"day","type":"long","nullable":true},{"name":"hour","type":"long","nullable":true},{"name":"minute","type":"long","nullable":true}]}\n'
SCHEMA_FILEDSAND_TYPES = """creation_time string,
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


def test_kv_table():
    logger = Logger()
    conf = AppConf(logger, "test.ini")
    kvtable = KVTable(logger, conf, KVTABLE_NAME)
    kvtable.import_table_schema()
    assert kvtable.name == KVTABLE_NAME
    assert kvtable.schema == SCHEMA
