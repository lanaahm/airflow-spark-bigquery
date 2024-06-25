import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

def top_employee_by_revenue(jdbc_url, jdbc_properties):
  spark = SparkSession.builder \
    .appName("Top Employee by Revenue") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.network.timeout", "800s") \
    .getOrCreate()
  
  try:
    order_details = spark.read.jdbc(url=jdbc_url, table="order_details", properties=jdbc_properties)
    orders = spark.read.jdbc(url=jdbc_url, table="orders", properties=jdbc_properties)
    employees = spark.read.jdbc(url=jdbc_url, table="employees", properties=jdbc_properties)
  except AnalysisException as e:
    print(f"Error reading data from JDBC source: {e}")
    spark.stop()
    return
  
  employee_revenue = orders.join(order_details, orders["orderID"] == order_details["orderID"], "inner") \
      .groupBy(
        F.date_format(F.date_trunc("month", orders["orderDate"]), "yyyy-MM-dd").alias("month"),
        orders["employeeID"]
      ).agg(
        F.sum((order_details["unitPrice"] - (order_details["unitPrice"] * order_details["discount"])) * order_details["quantity"]).alias("total_gross_revenue")
      )

  # Rank employees based on total gross revenue
  ranked_employees = employee_revenue.withColumn(
    "employee_rank",
    F.rank().over(Window.partitionBy("month").orderBy(F.desc("total_gross_revenue")))
  ).filter(
    F.col("employee_rank") == 1
  )

  # Join with employees to get final result
  final_result = ranked_employees.join(
    employees, ranked_employees["employeeID"] == employees["employeeID"], "inner"
  ).select(
    ranked_employees["month"],
    ranked_employees["employeeID"],
    F.concat(employees["lastName"], F.lit(", "), employees["firstName"]).alias("employeeName"),
    ranked_employees["total_gross_revenue"]
  )

  # Save the final result to CSV
  try:
    df = final_result.orderBy(F.asc("month"), F.desc("total_gross_revenue"))
    df = df.toPandas()
    df.to_csv(f"output/dm_top_employee_by_revenue.csv", sep=',', encoding='utf-8', index=False)
    print("Data successfully written to output/dm_top_employee_by_revenue.csv")
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
    top_employee_by_revenue(jdbc_url, jdbc_properties)