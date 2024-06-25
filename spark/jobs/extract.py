import sys
from pyspark.sql import SparkSession

def main(jdbc_url, jdbc_properties, source_name, output_table):
  try:
    # Initialize the Spark session
    spark = SparkSession.builder \
        .appName(f"PostgreSQL to PySpark {source_name}") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.network.timeout", "800s") \
        .getOrCreate()
    
    # Read data from PostgreSQL
    df = spark.read.jdbc(url=jdbc_url, table=source_name, properties=jdbc_properties)

    # Write DataFrame to csv file
    df = df.toPandas()
    df.to_csv(f"output/{output_table}.csv", sep=',', encoding='utf-8', index=False)
    spark.catalog.clearCache()
    spark.stop()
    print(f"Data successfully written to output/{output_table}.csv")
  
  except Exception as e:
    print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 6:
      print("Usage: script.py <jdbc_url> <user> <password> <source_name> <output_table>")
      sys.exit(1)

    jdbc_url = sys.argv[1]
    jdbc_properties = {
      "user": sys.argv[2],
      "password": sys.argv[3],
      "driver": "org.postgresql.Driver"
    }
    source_name = sys.argv[4]
    output_table = sys.argv[5]
    main(jdbc_url, jdbc_properties, source_name, output_table)
