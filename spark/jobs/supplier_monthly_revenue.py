import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

def supplier_monthly_revenue(jdbc_url, jdbc_properties):
  # Create a Spark session
  spark = SparkSession.builder \
    .appName("Supplier Monthly Revenue") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.network.timeout", "800s") \
    .getOrCreate()

  # Read data from JDBC source
  try:
    order_details = spark.read.jdbc(url=jdbc_url, table="order_details", properties=jdbc_properties)
    orders = spark.read.jdbc(url=jdbc_url, table="orders", properties=jdbc_properties)
    products = spark.read.jdbc(url=jdbc_url, table="products", properties=jdbc_properties)
    suppliers = spark.read.jdbc(url=jdbc_url, table="suppliers", properties=jdbc_properties)
  except AnalysisException as e:
    print(f"Error reading data from JDBC source: {e}")
    spark.stop()
    return

  # Perform necessary joins and aggregations
  order_details = order_details.alias("od")
  orders = orders.alias("o")
  products = products.alias("p")
  suppliers = suppliers.alias("s")

  order_details_join = orders.join(order_details, orders["orderID"] == order_details["orderID"], "inner")
  product_revenue = order_details_join.join(products, order_details_join["productID"] == products["productID"], "inner") \
      .select(
        F.date_trunc("month", order_details_join["orderDate"]).alias("orderMonth"),
        products["supplierID"],
        order_details_join["productID"],
        order_details_join["unitPrice"],
        order_details_join["quantity"],
        order_details_join["discount"],
        ((order_details_join["unitPrice"] - (order_details_join["unitPrice"] * order_details_join["discount"])) * order_details_join["quantity"]).alias("gross_revenue")
      )

  supplier_monthly_revenue = product_revenue.groupBy(
    F.date_format(F.date_trunc("month", product_revenue["orderMonth"]), "yyyy-MM-dd").alias("month"),
    product_revenue["supplierID"]
  ).agg(
    F.sum(product_revenue["gross_revenue"]).alias("total_gross_revenue")
  )

  final_result = supplier_monthly_revenue.join(
    suppliers, supplier_monthly_revenue["supplierID"] == suppliers["supplierID"], "inner"
  ).select(
    supplier_monthly_revenue["month"],
    suppliers["supplierID"],
    suppliers["companyName"].alias("supplierName"),
    supplier_monthly_revenue["total_gross_revenue"]
  )

  # Save the final result to CSV
  try:
    df = final_result.orderBy(F.asc("month"), F.desc("total_gross_revenue"))
    df = final_result.toPandas()
    df.to_csv(f"output/dm_supplier_monthly_revenue.csv", sep=',', encoding='utf-8', index=False)
    print("Data successfully written to output/dm_supplier_monthly_revenue.csv")
  except Exception as e:
    print(f"Error saving data to CSV: {e}")
  
  spark.catalog.clearCache()
  spark.stop()

if __name__ == "__main__":
  if len(sys.argv) != 4:
    print("Usage: spark-submit script.py <jdbc_url> <user> <password>")
    sys.exit(1)

  jdbc_url = sys.argv[1]
  jdbc_properties = {
    "user": sys.argv[2],
    "password": sys.argv[3],
    "driver": "org.postgresql.Driver"
  }
  supplier_monthly_revenue(jdbc_url, jdbc_properties)
