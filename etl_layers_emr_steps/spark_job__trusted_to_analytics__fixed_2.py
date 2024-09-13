from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# spconf = SparkConf().set('spark.ui.port', '4050')
# context = SparkContext(conf=spconf)
# context.stop()


spark = (
    SparkSession.builder
    .appName("Salic_Trusted_to_Analytics")
    .getOrCreate()
)

glue_database_name = 'salic_faker_trusted'
trusted_table_name = 'salic_projects'
trust_bucket = 'vrc-how-d2-salic-trusted'

analytics_bucket = 'vrc-how-d2-salic-analytics'

analytics_table_name_agg = 'salic_projects_agg'
analytics_table_name_time_values = 'salic_projects_time'



# df = (
#     spark.read 
#     .format("glue")
#     .option("database", glue_database_name)
#     .table(f"{glue_database_name}.{trusted_table_name}")
#     )



trusted_bucket_path = f"s3://{trust_bucket}/{trusted_table_name}/"

df = spark.read.parquet(trusted_bucket_path)
                    


dim_cols = ["uf_projeto","municipio_projeto","segmento","ano_projeto"]

metric_cols = ["valor_aprovado","valor_captado","valor_proposta"]

time_cols = ["data_inicio","data_termino"]


# Perform aggregation on time values
df_time_values = (
    df.groupBy(dim_cols + time_cols)
      .agg(
          *[sum(col(c)).alias(c) for c in metric_cols],  # summation of metrics
          count("*").alias("projects")  # Count projects
      )
)

# Perform aggregation across dimensions only
df_agg = (
    df_time_values.groupBy(dim_cols)
                  .agg(
                      *[sum(col(c)).alias(c) for c in metric_cols + ["projects"]]
                  )
)


partition_cols = ["data_inicio","uf_projeto"]


# Write the result back to S3 in Parquet format
s3_output_path_time_values = f"s3://{analytics_bucket}/{analytics_table_name_time_values}/"
df_time_values.write.partitionBy(partition_cols).mode("overwrite").parquet(s3_output_path_time_values)

s3_output_path_agg = f"s3://{analytics_bucket}/{analytics_table_name_agg}/"
df_agg.write.partitionBy("uf_projeto").mode("overwrite").parquet(s3_output_path_agg)

# Stop the Spark session
spark.stop()