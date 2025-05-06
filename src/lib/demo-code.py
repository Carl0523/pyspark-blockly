from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PySparkBlocklyApp") \
    .getOrCreate()

# Get the SparkContext for RDD operations
sc = spark.sparkContext


def remove_header(rdd):
  # Extract the header
  header = rdd.first()
  # Filter out the header row
  data_without_header = rdd.filter(lambda line: line != header)
  return data_without_header


raw_data = sc.textFile('/data/aircrafts_data.csv')

data_no_header = remove_header(raw_data)

parsed_data = data_no_header.map(lambda x: x.split(","))

categorized_data = parsed_data.map(lambda x: (x[0], x[1], int(x[2]), "Long-range" if int(x[2]) > 5000 else "Medium-range" if int(x[2]) > 3000 else "Short-range"))

long_range_aircraft = categorized_data.filter(lambda x: x[3] == "Long-range")

category_pairs = long_range_aircraft.map(lambda x: (x[3], 1))

category_counts = category_pairs.reduceByKey(lambda a, b: a + b)

print(category_counts.collect())