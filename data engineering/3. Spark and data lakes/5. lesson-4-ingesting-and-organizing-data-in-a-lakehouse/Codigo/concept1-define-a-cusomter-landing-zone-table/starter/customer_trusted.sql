CREATE EXTERNAL TABLE `customer_trusted`(
  `customername` string COMMENT '', 
  `email` string COMMENT '', 
  `phone` string COMMENT '', 
  `birthday` string COMMENT '', 
  `serialnumber` string COMMENT '', 
  `registrationdate` bigint COMMENT '', 
  `lastupdatedate` bigint COMMENT '', 
  `sharewithresearchasofdate` bigint COMMENT '', 
  `sharewithpublicasofdate` bigint COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'TableType'='EXTERNAL_TABLE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://jh-data-lakehouse/customer/trusted/'
TBLPROPERTIES (
  'classification'='parquet')