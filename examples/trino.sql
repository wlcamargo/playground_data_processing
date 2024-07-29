CREATE SCHEMA hive.bronze WITH (location='s3a://bronze/')

CREATE SCHEMA hive.silver WITH (location='s3a://silver/')

CREATE SCHEMA hive.gold WITH (location='s3a://gold/')

CREATE TABLE hive.bronze.titles (
  id VARCHAR,
  title VARCHAR,
  type VARCHAR,
  description VARCHAR,
  release_year VARCHAR,
  age_certification VARCHAR,
  runtime VARCHAR,
  genres VARCHAR,
  production_countries VARCHAR,
  seasons VARCHAR,
  imdb_id VARCHAR,
  imdb_score VARCHAR,
  imdb_votes VARCHAR,
  tmdb_popularity VARCHAR,
  tmdb_score VARCHAR
) WITH (
  format='CSV',
  external_location='s3a://bronze/titles/',
  skip_header_line_count=1
)

CREATE TABLE hive.bronze.credits (
  person_id VARCHAR,
  id VARCHAR,
  name VARCHAR,
  character VARCHAR,
  role VARCHAR
) WITH (
  format='CSV',
  external_location='s3a://bronze/credits/',
  skip_header_line_count=1
)

-- Mapear tabela no trino
CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'delta_employee',
    table_location => 's3a://bronze/delta_employee'
);

describe delta.bronze.delta_employee

SHOW CREATE TABLE delta.bronze.delta_employee;

SELECT firstname, middlename, lastname, id, gender, salary FROM delta.bronze.delta_employee;

--Tabelas Adventureworks
--Customer Sales
 SELECT
  sc.customerid,
  soh.modifieddate, 
  sum(soh.totaldue) as totalsales,
  soh.month_key
FROM
  delta.bronze.bronze_sales_salesorderheader soh 
  INNER JOIN delta.bronze.bronze_sales_customer sc ON soh.customerid = sc.customerid
  INNER JOIN delta.bronze.bronze_sales_store ss ON sc.storeid = ss.businessentityid
GROUP BY 
  soh.modifieddate,
  sc.customerid,
  soh.month_key
  

--Simulation incremental update

select * from humanresources.department

insert into humanresources.department values (17, 'Business Intelligence', 'Technology', '2024-04-30 00:00:00.000');

insert into humanresources.department values (18, 'Data Engineer', 'Technology', '2024-05-02 00:00:00.000');

insert into humanresources.department values (19, 'Data Scientist', 'Technology', '2024-06-01 00:00:00.000');

insert into humanresources.department values (20, 'Data Architect', 'Technology', '2024-06-02 00:00:00.000');

insert into humanresources.department values (21, 'Tech Lead', 'Technology', '2024-06-03 00:00:00.000');

DELETE FROM humanresources.department
WHERE departmentid >= 17;


-- for time travel
update delta.bronze.delta_employee 
set firstname = 'chapolin'

insert into delta.bronze.delta_employee values ('wallace', 'Camargo', 'Graca', '77777', 'M', 50000)


-- bronze tables
CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_humanresources_department',
    table_location => 's3a://bronze/adventure_works/bronze_humanresources_department'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_humanresources_employee',
    table_location => 's3a://bronze/adventure_works/bronze_humanresources_employee'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_sales_countryregioncurrency',
    table_location => 's3a://bronze/adventure_works/bronze_sales_countryregioncurrency'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_sales_currency',
    table_location => 's3a://bronze/adventure_works/bronze_sales_currency'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_sales_currencyrate',
    table_location => 's3a://bronze/adventure_works/bronze_sales_currencyrate'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_sales_customer',
    table_location => 's3a://bronze/adventure_works/bronze_sales_customer'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_sales_personcreditcard',
    table_location => 's3a://bronze/adventure_works/bronze_sales_personcreditcard'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_sales_salesorderdetail',
    table_location => 's3a://bronze/adventure_works/bronze_sales_salesorderdetail'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_sales_salesorderheader',
    table_location => 's3a://bronze/adventure_works/bronze_sales_salesorderheader'
);

CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'bronze_sales_store',
    table_location => 's3a://bronze/adventure_works/bronze_sales_store'
);


-- silver tables

CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_humanresources_department',
    table_location => 's3a://silver/adventure_works/silver_humanresources_department'
);

CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_humanresources_employee',
    table_location => 's3a://silver/adventure_works/silver_humanresources_employee'
);


CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_sales_countryregioncurrency',
    table_location => 's3a://silver/adventure_works/silver_sales_countryregioncurrency'
);

CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_sales_currency',
    table_location => 's3a://silver/adventure_works/silver_sales_currency'
);


CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_sales_currencyrate',
    table_location => 's3a://silver/adventure_works/silver_sales_currencyrate'
);

CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_sales_customer',
    table_location => 's3a://silver/adventure_works/silver_sales_customer'
);

CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_sales_personcreditcard',
    table_location => 's3a://silver/adventure_works/silver_sales_personcreditcard'
);

CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_sales_salesorderdetail',
    table_location => 's3a://silver/adventure_works/silver_sales_salesorderdetail'
);

CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_sales_salesorderheader',
    table_location => 's3a://silver/adventure_works/silver_sales_salesorderheader'
);

CALL delta.system.register_table(
    schema_name => 'silver',
    table_name => 'silver_sales_store',
    table_location => 's3a://silver/adventure_works/silver_sales_store'
);


--gold tables 

CALL delta.system.register_table(
    schema_name => 'gold',
    table_name => 'gold_humanresources_department',  
    table_location => 's3a://gold/adventure_works/gold_humanresources_department'
);


CALL delta.system.register_table(
    schema_name => 'gold',
    table_name => 'gold_humanresources_groupname_qtd',
    table_location => 's3a://gold/adventure_works/gold_humanresources_groupname_qtd'
);

--update sem where
update delta.bronze.delta_employee 
set firstname = 'chapolin'

select * from delta.bronze.delta_employee 

-- add sample delta dbt
CALL delta.system.register_table(
    schema_name => 'bronze',
    table_name => 'sample_table_dbt',
    table_location => 's3a://bronze/sample_dbt_delta_table'
);

select * from delta.bronze.sample_dbt_delta_table

