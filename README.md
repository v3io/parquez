
A mechanism for storing fresh/hot data in the NoSQL database
and historical data on Parquet while providing a single access for users (via a view) for easier access to real time and historical data

Users will be able to create a view for the “parquez” table using a script Rest call . Here are the parameters
View name
Partition by [h / m / d / y] - only time based partition is supported in this phase
Partition creation interval – 1 - 24h , 1-31d, 1-12m, 1-Ny.
KeyValue window – h, d,m, y 
Historical window – h, d,m, y 
Table name (the KV table for the view, need to specify the full path)
Schema path 
The view will be created in Presto based on Hive & V3IO KV 
Once the user creates the view an automated job is created by the interval given:
Job creates the view
Job deletes the old KV partitions & the old parquet files
Job will be running on the App nodes
Job is based on crontab
The job will generate “info” events when running and Major event upon failure (with a description of the error) - TBD Avi Asulin (either system events/dedicated logs)
