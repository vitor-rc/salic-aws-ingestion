CREATE EXTERNAL TABLE `salic_projects_time`(
  `municipio_projeto` string, 
  `segmento` string, 
  `ano_projeto` int, 
  `data_termino` date, 
  `valor_aprovado` bigint, 
  `valor_captado` bigint, 
  `valor_proposta` bigint, 
  `projects` bigint)
PARTITIONED BY ( 
  `data_inicio` string, 
  `uf_projeto` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://vrc-how-d2-salic-analytics/salic_projects_time/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crawler analytics', 
  'averageRecordSize'='67', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='28', 
  'partition_filtering.enabled'='true', 
  'recordCount'='7000', 
  'sizeKey'='351220', 
  'typeOfData'='file')