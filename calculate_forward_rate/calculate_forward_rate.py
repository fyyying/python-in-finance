# Implementation with PySpark
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as f

# Start with selecting the scenario to play with ["forward_rate", "forward_rate_with_group"]:
# forward_rate: the simple example without currency group
# forward_rate_group: the example when currency group is added
select_scenario = "forward_rate" #"forward_rate_with_group"


# Read in the data
if select_scenario == "forward_rate":
    rate_path = r"./Calculate fw data.csv"
elif select_scenario == "forward_rate_with_group":
    rate_path = r"./Calculate fw data group.csv"
else:
    raise ValueError("No valid scenario given")

# Define Spark settings
builder = SparkSession.builder.appName("Koalas")
builder = builder.config("spark.sql.execution.arrow.enabled", "true")\
.config("spark.sql.adaptive.enabled", "true")\
.config("spark.sql.optimizer.nestedSchemaPruning.enabled", "true")\
.config("spark.sql.optimizer.dynamicPartitionPruning", "true")\

spark = builder.getOrCreate()

# Read in the spot rate data
data = spark.read.csv(rate_path, header=True, inferSchema=True)

# Shift the discount factor by 1 period to forward the reserve to corresponding period
if select_scenario == "forward_rate_group":
    window = Window.partitionBy("Currency").orderBy("Maturity")
else:
    window = Window.orderBy("Maturity")
data = data.withColumn("Spot_Rate_Shift", f.lag(f.col("Spot_Rate"), 1).over(window)) \
           .fillna(0, subset=["Spot_Rate_Shift"])
data = data.withColumn("Forward_Rate", (1 + f.col("Spot_Rate"))**f.col("Maturity") /
                                       ((1 + f.col("Spot_Rate_Shift"))**(f.col("Maturity") - 1)) - 1)

# Implementation with Pandas
import pandas as pd

# Read in the data from csv
data = pd.read_csv(rate_path)

# Shift the spot rate down along the maturity axis and calculate forward rate
if select_scenario == "forward_rate_group":
    data["Spot_Rate_Shift"] = data.groupby("Currency")["Spot_Rate"].shift(1, fill_value=0)
else:
    data["Spot_Rate_Shift"] = data["Spot_Rate"].shift(1, fill_value=0)
data["Forward_Rate"] = (((1 + data["Spot_Rate"]) ** data["Maturity"]) /
                       ((1 + data["Spot_Rate_Shift"]) ** (data["Maturity"] - 1)) - 1)


# Implementation with Koalas
import databricks.koalas as ks
# ks.set_option("compute.ops_on_diff_frames", True)

# Read in the data from csv
data = ks.read_csv(rate_path)

# Shift the spot rate down along the maturity axis and calculate forward rate
if select_scenario == "forward_rate_group":
    data["Spot_Rate_Shift"] = data.groupby("Currency")["Spot_Rate"].shift(1, fill_value=0)
else:
    data["Spot_Rate_Shift"] = data["Spot_Rate"].shift(1, fill_value=0)
data["Forward_Rate"] = (((1 + data["Spot_Rate"]) ** data["Maturity"]) /
                       ((1 + data["Spot_Rate_Shift"]) ** (data["Maturity"] - 1)) - 1)

print("You have reached the end, well done!")