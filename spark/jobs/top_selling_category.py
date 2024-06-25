import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

def top_selling_category(jdbc_url, jdbc_properties):
  # Create a Spark session
  spark = SparkSession.builder \
    .appName("Top Selling Category") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.network.timeout", "800s") \
    .getOrCreate()

  try:
    order_details = spark.read.jdbc(url=jdbc_url, table="order_details", properties=jdbc_properties)
    orders = spark.read.jdbc(url=jdbc_url, table="orders", properties=jdbc_properties)
    products = spark.read.jdbc(url=jdbc_url, table="products", properties=jdbc_properties)
    categories = spark.read.jdbc(url=jdbc_url, table="categories", properties=jdbc_properties)
  except AnalysisException as e:
    print(f"Error reading data from JDBC source: {e}")
    spark.stop()
    return
  # Perform the equivalent operations in PySpark
  product_revenue = orders.join(order_details, orders["orderID"] == order_details["orderID"], "inner") \
      .join(products, order_details["productID"] == products["productID"], "inner") \
      .select(
        F.date_trunc("month", orders["orderDate"]).alias("orderMonth"),
        products["categoryID"],
        order_details["productID"],
        order_details["unitPrice"],
        order_details["quantity"],
        order_details["discount"],
        ((order_details["unitPrice"] - (order_details["unitPrice"] * order_details["discount"])) * order_details["quantity"]).alias("gross_revenue")
      )

  # Aggregate to get category_monthly_revenue
  category_monthly_revenue = product_revenue.groupBy(
    F.date_format(F.date_trunc("month", product_revenue["orderMonth"]), "yyyy-MM-dd").alias("month"),
    product_revenue["categoryID"]
  ).agg(
    F.sum(product_revenue["gross_revenue"]).alias("total_gross_revenue")
  )

  # Rank categories based on total gross revenue
  ranked_categories = category_monthly_revenue.withColumn(
    "category_rank",
    F.rank().over(Window.partitionBy("month").orderBy(F.desc("total_gross_revenue")))
  ).filter(
    F.col("category_rank") == 1
  )

  # Join with categories to get final result
  final_result = ranked_categories.join(
    categories, ranked_categories["categoryID"] == categories["categoryID"], "inner"
  ).select(
    ranked_categories["month"],
    ranked_categories["categoryID"],
    categories["categoryName"],
    ranked_categories["total_gross_revenue"]
  )

  try:
    # Show or save the final result
    df = final_result.orderBy(F.asc("month"), F.desc("total_gross_revenue"))
    df = df.toPandas()
    df.to_csv(f"output/dm_top_selling_category.csv", sep=',', encoding='utf-8', index=False)
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
    top_selling_category(jdbc_url, jdbc_properties)