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
            .appName("Refinement Incremental Silver to Gold AdventureWorks") \
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

    logging.info("Starting Refinement...")
    
    spark = configure_spark()
    input_name = configs.prefix_layer_name['2']  # bronze layer
    hdfs_input = configs.lake_path['silver']
    
    output_name = configs.prefix_layer_name['3']  # silver layer
    hdfs_output = configs.lake_path['gold']

    for table_name, query in configs.tables_gold.items():        
        try:
            
            query = F.get_query(table_name, hdfs_input, input_name, configs.tables_gold)
            
            df_input = spark.sql(query)
            
            df_output = spark.read.format("delta").load(f'{hdfs_output}{output_name}{table_name}')

            max_modified_date_delta = df_output.selectExpr("max(modifieddate)").collect()[0][0]

            df_new_data = df_input.filter(df_input["modifieddate"] > max_modified_date_delta)
            
            df_with_update_date = df_new_data.withColumn("last_update", lit(datetime.now()))

            df_with_update_date.write.format("delta").mode("append").partitionBy('month_key').save(f'{hdfs_output}{output_name}{table_name}')

            num_rows_written = df_with_update_date.count()
            logging.info(f"Table {table_name} successfully processed and saved to Delta Lake: {hdfs_output}{output_name}{table_name}. {num_rows_written} rows written.")

        except Exception as e:
            logging.error(f"Error processing table {table_name}: {str(e)}")

    logging.info("Refinement completed!")

if __name__ == "__main__":
    ingest_data()
