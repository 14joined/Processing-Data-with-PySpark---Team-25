from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("DataExploration").getOrCreate()
schema = "polarity FLOAT, id LONG, date_time STRING, query STRING, user STRING, text STRING"

INPUT_TEST = "<...>/test_data.csv"
INPUT_TRAINING = "<...>/train_data.csv"

OUTPUT_PATH = "<...>/RAW"

spark_reader = spark.read.schema(schema)

# test_data = spark_reader.csv(INPUT_TEST, header=False)
training_data = spark_reader.csv(INPUT_TRAINING, header=False).cache()

print("xxxxxxxxxxxxxxxxxx From acquire_data.py training_data.count() = " + str(training_data.count()) + "yyyyyzzzzz")

# test_data.repartition(20).write.partitionBy("polarity").csv(OUTPUT_DIR, mode="overwrite")
training_data.repartition(20).write.partitionBy("polarity").csv(OUTPUT_PATH, mode="overwrite")

# spark.stop()
