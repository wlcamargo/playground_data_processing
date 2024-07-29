import pyspark
from pyspark.sql import SparkSession
import logging
from configs import configs
from functions import functions as F
import time

def get_credential_source():
    """Get the credentials for the data source."""
    credential_source = configs.credential_jdbc_postgres_adventureworks
    return {
        'url': credential_source['url'],
        'user': credential_source['user'],
        'password': credential_source['password'],
        'driver': credential_source['driver']
    }

def get_data(table_input_name, spark, credential_source):
    """Get data from PostgreSQL table and return a DataFrame."""
    return spark.read \
        .format("jdbc") \
        .option("url", credential_source['url']) \
        .option("user", credential_source['user']) \
        .option("dbtable", table_input_name) \
        .option("password", credential_source['password']) \
        .option("driver", credential_source['driver']) \
        .load()

def compare_data(df_input_data, storage_output, spark):
    """Compare data from source with target and return DataFrame with new records."""

    df_output = spark.read.format("parquet").load(storage_output)
    
    max_date_storage_output = df_output.selectExpr("max(modifieddate)").collect()[0][0]
    
    new_records_df = df_input_data.filter(df_input_data["modifieddate"] > max_date_storage_output)
    
    return new_records_df

def process_table(table_name, df_input_data, storage_output, spark):
    """Process table by adding metadata and inserting new records."""
    logging.info(f"Processing table: {table_name}. Source: PostgreSQL, Target: Delta Lake.")
    start_time = time.time()
    
    df_with_metadata = F.add_metadata(df_input_data)
    
    df_with_month_key = F.add_month_key_column(df_with_metadata, 'modifieddate')
    
    new_records_df = compare_data(df_with_month_key, storage_output, spark)
    
    if new_records_df.rdd.isEmpty():
        logging.info(f"No new data found for table {table_name}. Skipping ingestion.")
    else:
        num_rows_written = write_data(new_records_df, storage_output, spark)
        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"Table {table_name} processed in {duration:.2f} seconds. {num_rows_written} new rows written.")
    
def write_data(df_with_month_key, storage_output, spark):
    """Save new partitioned data to target and return the number of rows written."""
    start_time = time.time()
    df_with_month_key.write.format("parquet").mode("append").partitionBy('month_key').save(storage_output)
    num_rows_written = df_with_month_key.count()
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"New data processed and appended to target for table {storage_output} in {duration:.2f} seconds. {num_rows_written} rows written.")
    return num_rows_written

def main(app_name, source_name, target_name, spark_configs):

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info(f"Starting ingestions from {source_name} to {target_name}...")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", spark_configs['s3_endpoint']) \
        .config("spark.hadoop.fs.s3a.access.key", spark_configs['s3_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", spark_configs['s3_secret_key']) \
        .config("spark.hadoop.fs.s3a.path.style.access", spark_configs['s3_path_style_access']) \
        .config("spark.hadoop.fs.s3a.impl", spark_configs['s3_impl']) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", spark_configs['s3_credentials_provider']) \
        .config("hive.metastore.uris", spark_configs['hive_metastore_uris']) \
        .config("spark.sql.extensions", spark_configs['delta_sql_extension']) \
        .config("spark.sql.catalog.spark_catalog", spark_configs['delta_catalog']) \
        .getOrCreate()

    credential_source = get_credential_source() 
    
    for table_name in configs.tables_postgres_adventureworks.values():
        try:
            table_name_hdfs = F.convert_table_name(table_name)
            df_input_data = get_data(table_name, spark, credential_source)  
            lake_path = configs.lake_path['landing_zone_adventure_works']
            storage_output = f"{lake_path}{table_name_hdfs}"
            
            process_table(table_name, df_input_data, storage_output, spark)
        except Exception as e:

            logging.error(f"Error processing table {table_name}: {str(e)}")

    logging.info(f"Ingestions from {source_name} to {target_name} completed!")


if __name__ == "__main__":
    app_name = 'ELT Incremental Postgres to Landing AdventureWorks'
    source_name = 'Postgres - AdventureWorks'
    target_name = 'S3 Data Lake'
    spark_configs = configs.spark_configs_s3
    
    main(app_name, source_name, target_name, spark_configs)
