import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime  

from pyspark.sql.functions import lit  

from configs import configs
from functions import functions as F

def configure_spark():
    """Configure SparkSession."""
    spark = SparkSession.builder \
            .appName("ELT Incremental Landing to Bronze AdventureWorks") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "chapolin") \
            .config("spark.hadoop.fs.s3a.secret.key", "mudar@123") \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("hive.metastore.uris", "thrift://metastore:9083") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    return spark

def ingest_data():
    """Ingest data from AdventureWorks to HDFS."""

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Ingestion Delta...")

    table_input_name = configs.lake_path['landing_zone_adventure_works']
    output_prefix_layer_name = configs.prefix_layer_name['1']  # bronze layer
    storage_output = configs.lake_path['bronze']

    for key, value in configs.tables_postgres_adventureworks.items():
        table = value
        table_name = F.convert_table_name(table)

        try:
            df_input_data = spark.read.format("parquet").load(f'{table_input_name}{table_name}')

            df_delta = spark.read.format("delta").load(f'{storage_output}{output_prefix_layer_name}{table_name}')

            max_modified_date_delta = df_delta.selectExpr("max(modifieddate)").collect()[0][0]

            df_new_data = df_input_data.filter(df_input_data["modifieddate"] > max_modified_date_delta)
            
            df_with_update_date = df_new_data.withColumn("last_update", lit(datetime.now()))

            df_with_update_date.write.format("delta").mode("append").partitionBy('month_key').save(f'{storage_output}{output_prefix_layer_name}{table_name}')

            num_rows_written = df_with_update_date.count()
            logging.info(f"Table {table_name} successfully processed and saved to Delta Lake: {storage_output}{output_prefix_layer_name}{table_name}. {num_rows_written} rows written.")

        except Exception as e:
            logging.error(f"Error processing table {table}: {str(e)}")

    logging.info("Ingestion Delta completed!")

if __name__ == "__main__":
    spark = configure_spark()
    ingest_data()
