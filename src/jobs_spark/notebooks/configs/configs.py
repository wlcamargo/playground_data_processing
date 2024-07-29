from .host import host

spark_configs_s3 = {
    's3_endpoint': "http://minio:9000",
    's3_access_key': "chapolin",
    's3_secret_key': "mudar@123",
    's3_path_style_access': True,
    's3_impl': "org.apache.hadoop.fs.s3a.S3AFileSystem",
    's3_credentials_provider': "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    'hive_metastore_uris': "thrift://metastore:9083",
    'delta_sql_extension': "io.delta.sql.DeltaSparkSessionExtension",
    'delta_catalog': "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}

credential_postgres_adventureworks = {
    'host': host,
    'dbname': 'Adventureworks',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5435
}

credential_jdbc_postgres_adventureworks = {
    'format' : 'jdbc',
    'url': f'jdbc:postgresql://{host}:5435/Adventureworks',
    'user': 'postgres',
    'password': 'postgres',
    'driver': 'org.postgresql.Driver'
} 
    
lake_path = {
    'landing_zone_adventure_works': 's3a://landing-zone/adventure_works/',
    'bronze' : 's3a://bronze/adventure_works/',
    'silver' : 's3a://silver/adventure_works/',
    'gold' : 's3a://gold/adventure_works/'
}

prefix_layer_name = {
    '1': 'bronze_',
    '2': 'silver_',
    '3': 'gold_'
}

### Start Bronze Tables ###
tables_postgres_adventureworks = {
    '1': 'sales.countryregioncurrency',
    #'2': 'sales.creditcard',
    #'3': 'sales.currency',
    #'4': 'sales.currencyrate',
    #'5': 'sales.customer',
    #'6': 'sales.personcreditcard',
    #'7': 'sales.salesorderdetail',
    #'8': 'sales.salestaxrate',  
    #'9': 'sales.salesorderheadersalesreason',
    #'10': 'sales.salesperson',
    #'11': 'sales.salespersonquotahistory',
    #'12': 'sales.salesreason',
    #'13': 'sales.salestaxrate',
    #'14': 'sales.salesterritory',
    #'15': 'sales.salesterritoryhistory',
    #'16': 'sales.shoppingcartitem',
    #'17': 'sales.specialoffer',
    #'18': 'sales.specialofferproduct',
    #'19': 'sales.store',
    '20': 'humanresources.department',
    '21': 'humanresources.employee',
    '22': 'sales.salesorderheader'    
}

tables_queries_silver = {
    
'sales_countryregioncurrency': 
f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_countryregioncurrency`",
    
'humanresources_department': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}humanresources_department`",

'humanresources_employee': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}humanresources_employee`",

'sales_salesorderheader': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesorderheader`"
}


'''
tables_queries_silver_full = {
    
'sales_countryregioncurrency': 
f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_countryregioncurrency`",
    
'sales_creditcard': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_creditcard`",

'sales_currency': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_currency`",

'sales_currencyrate': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_currencyrate`",

'sales_customer': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_customer`",

'sales_personcreditcard': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_personcreditcard`",

'sales_salesorderdetail': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesorderdetail`",

'sales_salestaxrate': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salestaxrate`",

'sales_salesorderheadersalesreason': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesorderheadersalesreason`",

'sales_salesperson': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesperson`",

'sales_salespersonquotahistory': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salespersonquotahistory`",

'sales_salesreason': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesreason`",

'sales_salesterritory': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesterritory`",

'sales_salesterritoryhistory': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesterritoryhistory`",

'sales_shoppingcartitem': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_shoppingcartitem`",

'sales_specialoffer': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_specialoffer`",

'sales_specialofferproduct': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_specialofferproduct`",

'sales_store': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_store`",

'humanresources_department': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}humanresources_department`",

'humanresources_employee': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}humanresources_employee`",

'sales_salesorderheader': 
    f"SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesorderheader`"
}
'''
### Start Gold Tables ###

tables_gold = {   
    
# Humanresources Department
"humanresources_department": """
SELECT 
    departmentid, 
    name, 
    groupname, 
    modifieddate, 
    last_update, 
    month_key 
FROM 
    delta.`s3a://silver/adventure_works/silver_humanresources_department`;
    """,

# Qtd Humanresources Group Name  
"humanresources_groupname_qtd": """
SELECT 
    groupname, 
    modifieddate,
    last_update, 
    month_key, 
    count(*) as qtd
FROM 
    delta.`s3a://silver/adventure_works/silver_humanresources_department`
group by 
	groupname,
	modifieddate,
    last_update, 
    month_key ;
    """      
    
}