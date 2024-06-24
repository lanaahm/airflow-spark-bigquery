import os
import pandas as pd

from airflow.hooks.postgres_hook import PostgresHook

def extract_data(source_url, table_name):
  """Extract data from a table in the OLTP database."""
  if table_name == "territories" or table_name == "employee_territories":
    df = pd.read_csv(f"{source_url}", dtype={"territoryID": "str"})
  else:
    df = pd.read_csv(f"{source_url}")
  print(f"Extract Data {source_url} Success")
  return df

def get_unique_key(table_name):
  """Retrieve the unique key of the table."""
  unique_keys = {
    "regions": "regionID",
    "territories": "territoryID",
    "suppliers": "supplierID",
    "shippers": "shipperID",
    "categories": "categoryID",
    "products": "productID",
    "customers": "customerID",
    "employees": "employeeID",
    "orders": "orderID",
    "order_details": ["orderID", "productID", "unitPrice", "quantity", "discount"],
    "employee_territories": ["employeeID", "territoryID"]
  }
  if table_name in unique_keys:
    return unique_keys[table_name]
  else:
    raise ValueError("Table name not recognized.")

def remove_duplicate_data(new_data, existing_data, unique_key):
  """Remove duplicates from new data based on existing data."""
  if isinstance(unique_key, list):
    unique_values = set(map(tuple, existing_data[unique_key].values))
    return new_data[~new_data.apply(lambda row: tuple(row[unique_key]) in unique_values, axis=1)]
  else:
    unique_values = set(existing_data[unique_key])
    return new_data[~new_data[unique_key].isin(unique_values)]

def load_data(df, table_name):
  """Load the transformed data into the target table in the data warehouse."""
  try:

    target_hook = PostgresHook(postgres_conn_id='postgres-conn')
    target_conn = target_hook.get_sqlalchemy_engine()
    unique_key = get_unique_key(table_name)
    
    if isinstance(unique_key, list):
      unique_key_str = ', '.join([f'"{key}"' for key in unique_key])
    else:
      unique_key_str = f'"{unique_key}"'
        
    df_load = pd.read_sql(f"SELECT {unique_key_str} FROM {table_name}", target_conn)
    df = remove_duplicate_data(df, df_load, unique_key)
    df.to_sql(table_name, target_conn, index=False, if_exists='append', method='multi')
    print(f"Load Data {table_name} Success")

  except Exception as e:
    print(f"Error loading data into {table_name}: {e}")

def main(source_url, table_name):
    print("[Extract] Start")
    print(f"[Extract] Unzip data from '{source_url}' to '{table_name}'")

    source_data = extract_data(source_url, table_name)
    load_data(source_data, table_name)

    print(f"[Extract] End")