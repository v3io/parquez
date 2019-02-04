# Parquez

A mechanism for storing fresh/hot data in the NoSQL database
and historical data on Parquet while providing a single access for users (via a view) for easier access to real time and historical data

Users will be able to create a view for the “parquez” table using a script Rest call .
script parameters
View name  <br />
Partition by [h / m / d / y] - only time based partition is supported in this phase  <br />
Partition creation interval – 1 - 24h , 1-31d, 1-12m, 1-Ny.  <br />
KeyValue window – h, d,m, y  <br />
Historical window – h, d,m, y  <br />
Table name (the KV table for the view, need to specify the full path)  <br />
config file path   <br />

The view will be created in Presto based on Hive & V3IO KV 
Once the user creates the view an automated job is created by the interval given:
Job creates the view
Job deletes the old KV partitions & the old parquet files
Job will be running on the App nodes
Job is based on crontab
The job will generate “info” events when running and Major event upon failure (with a description of the error) - TBD Avi Asulin (either system events/dedicated logs)

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


