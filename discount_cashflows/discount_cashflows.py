path_cashflow = r"./Cashflows.csv"
path_rate = r"./Yield curve.csv"

# Implementation with Pandas
import pandas as pd

# Read in the cash flows data and rate data as csv
cashflow_df = pd.read_csv(path_cashflow)
rate_df = pd.read_csv(path_rate)

# Calculate discount factor from the rates
rate_df["Discount factor"] = 1 / (1 + rate_df["Interest rate"])**rate_df["Year"]

# Join cash flows with rates
cf_with_rate_df = cashflow_df.merge(rate_df, on=["Currency", "Year"], how="left")

# Calculate present values
cf_with_rate_df["Present value"] = cf_with_rate_df["Cash flows"] * cf_with_rate_df["Discount factor"]

# Groupby product and check the profitability
cf_with_rate_df = cf_with_rate_df.groupby("Product")[["Present value"]].sum().reset_index()

# -----

# Implementation with Koalas
import databricks.koalas as ks

# Read in the cash flows data and rate data as csv
cashflow_df = ks.read_csv(path_cashflow)
rate_df = ks.read_csv(path_rate)

# Calculate discount factor from the rates
rate_df["Discount factor"] = 1 / (1 + rate_df["Interest rate"])**rate_df["Year"]

# Join cash flows with rates
cf_with_rate_df = cashflow_df.merge(rate_df, on=["Currency", "Year"], how="left")

# Calculate present values
cf_with_rate_df["Present value"] = cf_with_rate_df["Cash flows"] * cf_with_rate_df["Discount factor"]

# Groupby product and check the profitability
cf_with_rate_df = cf_with_rate_df.groupby("Product")[["Present value"]].sum().reset_index()

# -----

# Implementation with PySpark
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as f

# Define Spark settings
builder = SparkSession.builder.appName("Discount_Cashflows")
spark = builder.getOrCreate()

# Read in the cash flows data and rate data as csv
cashflow_df = spark.read.csv(path_cashflow, header=True, inferSchema=True)
rate_df = spark.read.csv(path_rate, header=True, inferSchema=True)

# Calculate discount factor from the rates
rate_df = rate_df.withColumn("Discount factor", 1 / (1 + rate_df["Interest rate"])**rate_df["Year"])

# Join cash flows with rates
cf_with_rate_df = cashflow_df.join(f.broadcast(rate_df), on=["Currency", "Year"], how="left")

# Calculate present values
cf_with_rate_df = cf_with_rate_df.withColumn("Present value", f.col("Cash flows") * f.col("Discount factor"))

# Groupby product and check the profitability
cf_with_rate_df = cf_with_rate_df.groupBy("Product").agg(f.sum("Present value").alias("Present value"))


