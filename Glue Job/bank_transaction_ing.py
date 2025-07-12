import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
from botocore.exceptions import ClientError
from pyspark.sql.functions import to_date, date_format, col
from pyspark.sql import DataFrame # Para type hinting em métodos

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'origin_location_s3', 'table_location_s3'])

origin_bucket = args['origin_location_s3']
table_bucket = args['table_location_s3']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3 = boto3.client('s3')

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

#######################
###     Métodos     ###
#######################

def max_object(bucket_name: str, s3) -> int:
    parts = bucket_name.split('//')[1].split('/', 1) # Divide em nome do bucket e o resto como prefixo
    bucket_name_only = parts[0]
    prefix = parts[1] if len(parts) > 1 else '' # O prefixo pode ser vazio se o path for só o bucket
    print("Verificando se ja existem dados no bucket")
    try:
        # Tenta listar apenas um objeto (max_keys=1) para verificar a existência de qualquer coisa
        response = s3.list_objects_v2(Bucket=bucket_name_only, MaxKeys=1)

        # Se 'Contents' não existir ou estiver vazio, o bucket está vazio
        if 'Contents' not in response or not response['Contents']:
            print("Bucket não está vazio")
            return 0
        else:
            print("Bucket está vazio")
            return 1
    except ClientError as e:
        # Se ocorrer um erro, imprime e retorna -1
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "NoSuchBucket":
            print(f"Erro: O bucket '{bucket_name}' não existe.")
        elif error_code == "AccessDenied":
            print(f"Erro: Acesso negado ao bucket '{bucket_name}'. Verifique as permissões IAM.")
        else:
            print(f"Ocorreu um erro inesperado ao acessar o bucket '{bucket_name}': {e}")
        return -1
    except Exception as e:
        print(f"Ocorreu um erro genérico: {e}")
        return -1

def create_table_ddl(spark_df: DataFrame) -> str:
    pandas_df_sample = spark_df.limit(1).toPandas()
    
    schema_parts = []
    for column_name, dtype in pandas_df_sample.dtypes.items():

        dtype_str = str(dtype)
        sql_type = 'STRING' 

        if 'int' in dtype_str:
            sql_type = 'BIGINT'
        elif 'float' in dtype_str:
            sql_type = 'DOUBLE'
        elif 'bool' in dtype_str:
            sql_type = 'BOOLEAN'
        elif 'datetime' in dtype_str:
            sql_type = 'TIMESTAMP' 
        elif dtype_str == 'string': 
            sql_type = 'STRING'
        elif dtype_str == 'object':
            sql_type = 'STRING'
        
        schema_parts.append(f"`{column_name}` {sql_type}")
    
    table_ddl_columns = ",\n".join(schema_parts)
    return table_ddl_columns
    
def create_table(table_ddl: str, location: str, spark):
    query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS db_trusted.bank_transaction(
                {table_ddl}
            )
            PARTITIONED BY (
                `anomesdia` string
            )
            STORED AS PARQUET
            LOCATION '{location}'
            TBLPROPERTIES (
            'classification'='parquet',
            'projection.enabled'='true',
            'projection.anomesdia.type'='DATE', -- Supondo que anomesdia seja derivado de uma DATE
            'projection.anomesdia.range'='20240101,NOW', -- Ajuste conforme o range real de dados
            'projection.anomesdia.format'='yyyyMMdd', -- Formato da partição na pasta S3
            'projection.anomesdia.interval'='1',
            'projection.anomesdia.interval.unit'='DAYS'
        );
            """
    print("Executando a seguinte query para criação da tabela: \n" + query)
    spark.sql(query)
    print("Tabela db_trusted.bank_transaction criada com sucesso")

def read_csv_s3(origin_bucket, spark):
    print(f"Lendo arquivos CSV de: {origin_bucket}")
    df = spark.read.csv(
        origin_bucket,
        header=True,
        inferSchema=True,
        sep=",",
        quote='"',
        multiLine=False # Defina como True se seus campos puderem ter quebras de linha
    )
    print(f"DataFrame Spark criado com {df.count()} linhas e {len(df.columns)} colunas.")
    df.printSchema()
    
    return df

def transform_df(df: DataFrame) -> DataFrame:
    print("Convertendo 'TransactionDate' para o tipo DATE...")
    df_with_date = df.withColumn(
        "TransactionDate",
        to_date(col("TransactionDate"), "M/d/yy")
    )
    
    print("\nEsquema do DataFrame com coluna TransactionDate tratada:")
    df_with_date.printSchema()
    
    return df_with_date

def save_data(df: DataFrame, s3_output_path, spark):
    print("Criando coluna de anomesdia")
    df = df.withColumn(
        "anomesdia",
        date_format(col("TransactionDate"), "yyyyMMdd")
    )
    
    print(f"Salvando DataFrame em Parquet particionado por anomesdia em: {s3_output_path}")
    df.write \
      .mode('overwrite') \
      .partitionBy('anomesdia') \
      .parquet(s3_output_path)
    print(f"Dados salvos com sucesso em {s3_output_path} no modo overwrite.")
    
    repair_query = f"MSCK REPAIR TABLE `db_trusted`.`bank_transaction`"
    print(f"Executando MSCK REPAIR TABLE: {repair_query}")
    
    spark.sql(repair_query)
    print(f"MSCK REPAIR TABLE para `db_trusted`.`bank_transaction` concluído.")

if __name__ == "__main__":
    is_bucket_empty = max_object(table_bucket, s3)
    df = read_csv_s3(origin_bucket, spark)
    df = transform_df(df)
    
    if is_bucket_empty == 1: 
        table_ddl = create_table_ddl(df)
        create_table(table_ddl, table_bucket, spark)
    
    save_data(df, table_bucket, spark)
job.commit()