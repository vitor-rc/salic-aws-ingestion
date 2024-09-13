CREATE EXTERNAL TABLE `salic_projects`(
  `nome_projeto` string, 
  `ano_projeto` int, 
  `area_projeto` string, 
  `municipio_projeto` string, 
  `segmento` string, 
  `palavra` string, 
  `produtora` string, 
  `proponente` string, 
  `objetivos` string, 
  `ficha_tecnica` string, 
  `justificativa` string, 
  `data_termino` date, 
  `valor_aprovado` int, 
  `valor_captado` int, 
  `valor_proposta` int)
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
  's3://vrc-how-d2-salic-trusted/salic_projects/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crawler - trusted zone', 
  'averageRecordSize'='580', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='54', 
  'partition_filtering.enabled'='true', 
  'recordCount'='7000', 
  'sizeKey'='2285000', 
  'typeOfData'='file')