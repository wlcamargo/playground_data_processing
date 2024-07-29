from pyspark.sql.functions import unix_timestamp, lit, date_format
from pyspark.sql.types import IntegerType
from datetime import datetime
import pyspark
from pyspark.sql.types import IntegerType, TimestampType, DoubleType

def convert_table_name(table_name):
    """Função usada para converter o nome das tabelas, ou seja, onde tem . trocar por _"""
    return table_name.replace(".", "_")

def create_temp_view(df, view_name):
    """
    Cria uma TempView a partir de um DataFrame.

    :param df: DataFrame a partir do qual a TempView será criada.
    :param view_name: Nome da TempView.
    """
    df.createOrReplaceTempView(view_name)

def add_partition_date_column(df, date_column_name):
    """
    Add a partition date column to the DataFrame.
    
    Parameters:
        df (pyspark.sql.DataFrame): Input DataFrame.
        date_column_name (str): Name of the date column.
        
    Returns:
        pyspark.sql.DataFrame: DataFrame with partition date column added.
    """
    df_with_partition_date = df.withColumn("partition_date", 
                                           unix_timestamp(df[date_column_name], 'yyyy-MM-dd').cast(IntegerType()))
    return df_with_partition_date

def add_metadata(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Add metadata columns to the DataFrame.
    
    Parameters:
        df (pyspark.sql.DataFrame): Input DataFrame.
        
    Returns:
        pyspark.sql.DataFrame: DataFrame with metadata columns added.
    """
    # Add last_update column with current timestamp
    df_with_metadata = df.withColumn("last_update", lit(datetime.now()))
    
    return df_with_metadata

def add_month_key_column(df, date_column_name):
    """
    Add a month key column to the DataFrame using the year and month from the specified date column.
    
    Parameters:
        df (pyspark.sql.DataFrame): Input DataFrame.
        date_column_name (str): Name of the date column.
        
    Returns:
        pyspark.sql.DataFrame: DataFrame with month key column added.
    """
    df_with_month_key = df.withColumn("month_key", date_format(df[date_column_name], "yyyyMM").cast(IntegerType()))
    return df_with_month_key

def get_query(table_name, hdfs_source, prefix_layer_name_source, tables_queries):
    """Busca a query que será utilizada em um dicionário"""
    if table_name in tables_queries:
        return tables_queries[table_name].format(hdfs_source=hdfs_source, prefix_layer_name_source=prefix_layer_name_source)
    else:
        raise ValueError(f"No query found for table: {table_name}")

def load_data_from_postgres_jdbc(configs, source_table, spark):
    """Carrega os dados da tabela PostgreSQL para um DataFrame usando as configurações fornecidas."""
    return spark.read \
        .format(configs['format']) \
        .option("url", configs['url']) \
        .option("user", configs['user']) \
        .option("password", configs['password']) \
        .option("dbtable", source_table) \
        .option("driver", configs['driver']) \
        .load()
