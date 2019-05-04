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
partition-by [h / m / d / y] : only time based partition is supported in this phase  <br />
partition-interval : Partition creation interval – 1 - 24h , 1-31d, 1-12m, 1-Ny.  <br />
real-time-table-name : The KV table for the view, need to specify the full path)  <br />
real-time-window window [h, d,m, y] : The time window for storing data in key value (hot data) <br />
historical-retention [h, d,m, y] : The retention of all parquez data  <br />
config : config file path   <br />

### config file parametres
[v3io] <br />
v3io_container = bigdata <br />
access_key = <access_key> <br />

[hive] <br />
hive_schema = default <br />

[presto] <br />
uri = <localhost> <br />
v3io_connector = v3io <br />
hive_connector = hive <br />


[nginx] <br />
v3io_api_endpoint_host = <localhost> <br />
v3io_api_endpoint_port = 8081 <br />
username = <user_name> <br />
password = <password> <br />


### Prerequisites
1. parquez scripts
2. partitioned kv table 

### Building / deploying the functions

Clone this repository and `cd` into it:
```sh
mkdir parquez && \
    git clone https://github.com/iguazio/parquez.git && \
    cd parquez
```

Run the parquez
```
./run_parquez.sh --view-name parquezView --partition-by 1h --partition-interval 1h --real-time-window 3h \
--historical-retention 21h --real-time-table-name booking_service_kv --config config/parquez.ini
```


