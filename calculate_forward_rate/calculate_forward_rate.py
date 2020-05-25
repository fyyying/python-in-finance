rate_path = r"./Calculate fw data.csv"
rate_path_group = r"./Calculate fw data group.csv"


# Implementation with PySpark
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as f

# Define Spark settings
builder = SparkSession.builder.appName("Koalas")
# builder = builder.config("spark.sql.execution.arrow.enabled", "true")\
# .config("spark.sql.adaptive.enabled", "true")\
# .config("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")\
# .config("spark.sql.optimizer.dynamicPartitionPruning", "true")\

spark = builder.getOrCreate()

# Read in the spot rate data
data = spark.read.csv(rate_path, header=True, inferSchema=True)
# data = spark.read.csv(rate_path_group, header=True, inferSchema=True)

# Shift the discount factor by 1 period to forward the reserve to corresponding period
window = Window.orderBy("Maturity")
# window = Window.partitionBy("Currency").orderBy("Maturity")
data = data.withColumn("Spot_Rate_Shift", f.lag(f.col("Spot_Rate"), 1).over(window)) \
           .fillna(0, subset=["Spot_Rate_Shift"])
data = data.withColumn("Forward_Rate", (1 + f.col("Spot_Rate"))**f.col("Maturity") /
                                       ((1 + f.col("Spot_Rate_Shift"))**(f.col("Maturity") - 1)) - 1)

# Implementation with Pandas
import pandas as pd

# Read in the data from csv
data = pd.read_csv(rate_path)
# data = pd.read_csv(rate_path_group)

# Shift the spot rate down along the maturity axis and calculate forward rate
# data["Spot_Rate_Shift"] = data.groupby("Currency")["Spot_Rate"].shift(1, fill_value=0)
data["Spot_Rate_Shift"] = data["Spot_Rate"].shift(1, fill_value=0)
data["Forward_Rate"] = (((1 + data["Spot_Rate"]) ** data["Maturity"]) /
                       ((1 + data["Spot_Rate_Shift"]) ** (data["Maturity"] - 1)) - 1)


# Implementation with Koalas
import databricks.koalas as ks
# ks.set_option("compute.ops_on_diff_frames", True)

# Read in the data from csv
data = ks.read_csv(rate_path)
data = ks.read_csv(rate_path_group)

# Shift the spot rate down along the maturity axis and calculate forward rate
data["Spot_Rate_Shift"] = data.groupby("Currency")["Spot_Rate"].shift(1, fill_value=0)
data["Forward_Rate"] = (((1 + data["Spot_Rate"]) ** data["Maturity"]) /
                       ((1 + data["Spot_Rate_Shift"]) ** (data["Maturity"] - 1)) - 1)

print("DONE")