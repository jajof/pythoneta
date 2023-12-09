# Data Lineage in Airflow

Task dependencies can be used to reflect data lineage. In the example below, can you tell where data was loaded from? If a data analyst was concerned there was data missing in the location_traffic table, where would you start looking for the problem?

This is an example of using data lineage.

```
create_table >> load_from_s3_to_redshift >> calculate_location_traffic
```