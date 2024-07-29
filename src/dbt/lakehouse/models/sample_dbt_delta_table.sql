{{ config(
  materialized='table'
) }}

select 'I am a Delta table created by dbt.' as message