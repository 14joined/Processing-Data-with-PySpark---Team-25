from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from time import sleep

INPUT_PATH = "<...>"
OUTPUT_PATH = ""
timestampformat = "DDD MMM dd HH:mm:ss zzz yyyy"

spark = SparkSession.builder.appName("StructuredStreamingExample").getOrCreate()
schema = spark.read.json(INPUT_PATH).limit(10).schema

# regular spark reader
static_spark_reader = spark.read.schema(schema)

# streaming spark reader
stream_spark_reader = spark.readStream.schema(schema)

spark_reader = static_spark_reader

# spark_reader = stream_spark_reader

df = (
    spark_reader.json(INPUT_PATH)
    .select(
        f.col("data.id").alias("id"),
        f.col("data.text").alias("tweet"),
        f.col("matching_rules.tag").alias("tag")
    )
    .coalesce(1)
)

distinct_tweet_id = df.select(f.approx_count_distinct("id"), f.current_timestamp())

if not df.isStreaming:
    print("Plain old, basic DataFrame - meh!")
    df.show()
    distinct_tweet_id.show()
else:
    print("We are streaming!")
    stream_writer = (
        distinct_tweet_id.writeStream
        .queryName("tweet_id")
        .trigger(
            processingTime="5 seconds",
            # once=True
        )
        .outputMode("complete")
        .format("memory")
    )
    query = stream_writer.start()
    # stream_writer.awaitTermination()

for x in range(0, 60):
    print("Incoming data------------------------------------------->>>>>>>>>>>>>>>>>>>")
    spark.sql(f"SELECT * from {query.name}").show()
    sleep(3)
else:
    print("Live view ended. . .")

