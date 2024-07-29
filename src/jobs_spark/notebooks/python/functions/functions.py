import pandas as pd
from datetime import datetime
import re

def convert_table_name(table_name):
    """Convert table name by replacing '.' with '_'."""
    return table_name.replace(".", "_")


def add_partition_date_column(df, date_column_name):
    """
    Add a partition date column to the DataFrame.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
        date_column_name (str): Name of the date column.
        
    Returns:
        pd.DataFrame: DataFrame with partition date column added.
    """
    df['partition_date'] = pd.to_datetime(df[date_column_name]).astype(int) // 10**9  # Unix timestamp
    return df

def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add metadata columns to the DataFrame.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
        
    Returns:
        pd.DataFrame: DataFrame with metadata columns added.
    """
    # Add last_update column with current timestamp
    df['last_update'] = datetime.now()
    return df

def add_month_key_column(df, date_column_name):
    """
    Add a month key column to the DataFrame using the year and month from the specified date column.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
        date_column_name (str): Name of the date column.
        
    Returns:
        pd.DataFrame: DataFrame with month key column added.
    """
    df['month_key'] = pd.to_datetime(df[date_column_name]).dt.strftime('%Y%m').astype(int)
    return df

def get_query(table_name, hdfs_source, prefix_layer_name_source, tables_queries):
    """Retrieve the query to be used from a dictionary."""
    if table_name in tables_queries:
        return tables_queries[table_name].format(hdfs_source=hdfs_source, prefix_layer_name_source=prefix_layer_name_source)
    else:
        raise ValueError(f"No query found for table: {table_name}")

