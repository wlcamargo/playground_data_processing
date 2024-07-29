import pyspark
from pyspark.sql import SparkSession
import logging
from configs import configs
from functions import functions as F

spark = SparkSession.builder \
        .appName("ELT Full Postgres to Landing AdventureWorks") \
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


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


logging.info("Starting ingestions from AdventureWorks to Landing Zone...")

for table_input_name in configs.tables_postgres_adventureworks.values():
    try:
        table_input_path = F.convert_table_name(table_input_name)
        

        df_input_data = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://172.21.121.140:5435/Adventureworks") \
            .option("user", "postgres") \
            .option("dbtable", table_input_name) \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        output_table_name = configs.lake_path['landing_zone_adventure_works']
        output_table_path = f"{output_table_name}{table_input_path}"
        
        logging.info(f"Processing table: {table_input_path}")
        

        df_with_update_date = F.add_metadata(df_input_data)
        

        df_with_month_key = F.add_month_key_column(df_with_update_date, 'modifieddate')
            
        df_with_month_key.write.format("parquet").mode("overwrite").partitionBy('month_key').save(output_table_path)
        
        logging.info(f"Table {table_input_path} successfully processed and saved to HDFS: {output_table_path}")

    except Exception as e:

        logging.error(f"Error processing table {table_input_name}: {str(e)}")


logging.info("Ingestions to Landing Zone completed!")
