# Parquez

A mechanism for storing fresh/hot data in the NoSQL database
and historical data on Parquet while providing a single access for users (via a view) for easier access to real time and historical data

The view will be created in Presto based on Hive & V3IO KV 
Once the user creates the view an automated job is created by the interval given:
Job creates the view
Job deletes the old KV partitions & the old parquet files
Job will be running on the App nodes
Job is based on crontab

Users will be able to create a view for the “parquez” table using a script Rest call . <br />
### script parameters 
view-name : The unified view name (parquet and kv)  <br />
partition-by [h / d / m / y] : only time based partition is supported in this phase  <br />
partition-interval [1-24h / 1-31d / 1-12m / 1-Ny] : Partition creation interval .  <br />
real-time-table-name : The KV table for the view, need to specify the full path)  <br />
real-time-window window [h / d / m / y] : The time window for storing data in key value (hot data) <br />
historical-retention [h / d / m / y] : The retention of all parquez data  <br />
config : config file path   <br />

### config file parametres
[v3io] <br />
v3io_container = bigdata <br />

[hive] <br />
hive_schema = default <br />

[presto] <br />
uri = <pesto_uri> <br />
v3io_connector = v3io <br />
hive_connector = hive <br />


[nginx] <br />
v3io_api_endpoint_host = <v3io_api_endpoint_host> <br />
v3io_api_endpoint_port = 443 <br />

[compression] <br />
type = Parquet  <br />
coalesce = 6 <br />

### Prerequisites
1. parquez scripts
2. partitioned kv table 
3. enable hive in presto service
4. set hive.allow-drop-table=true 
5. set hive.non-managed-table-writes-enabled=true

### Building / deploying the functions

Clone this repository and `cd` into it:
```sh
mkdir parquez && \
    git clone https://github.com/iguazio/parquez.git && \
    cd parquez
```

Run parquez
```
from jupyter run parquez.ipynb

