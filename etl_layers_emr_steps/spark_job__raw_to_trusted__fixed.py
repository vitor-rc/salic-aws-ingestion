from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

# spconf = SparkConf().set('spark.ui.port', '4050')
# context = SparkContext(conf=spconf)
# context.stop()


spark = (
    SparkSession.builder
    .appName("Salic_Raw_to_Trusted")
    .getOrCreate()
)

raw_bucket = 'vrc-how-d2-salic-raw'
input_file = 'salic_projects_d2_ok.csv'

trust_bucket = 'vrc-how-d2-salic-trusted'
trusted_table_name = 'salic_projects'

s3_input_path = f"s3://{raw_bucket}/{input_file}"
df = spark.read.csv(s3_input_path, header=True, inferSchema=True)

def type_cast_columns(df, column_list, data_type):
  for col_name in column_list:
    df = df.withColumn(col_name, col(col_name).cast(data_type))
  return df

def fix_date_type(df, column_list):
  for col_name in column_list:
    df = df.withColumn(col_name, to_date(col_name))
  return df

int_cols = ["ano_projeto","valor_aprovado","valor_captado","valor_proposta"]
date_cols = ["data_inicio","data_termino"]

df_transform = type_cast_columns(df, int_cols, "int")
df_transform = fix_date_type(df_transform, date_cols)

partition_cols = ["data_inicio","uf_projeto"]


# Write the result back to S3 in Parquet format
s3_output_path = f"s3://{trust_bucket}/{trusted_table_name}/"
df_transform.write.partitionBy(partition_cols).mode("overwrite").parquet(s3_output_path)

# Stop the Spark session
spark.stop()