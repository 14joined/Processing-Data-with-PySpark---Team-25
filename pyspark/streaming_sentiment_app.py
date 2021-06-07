import html

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql import functions as f
from pyspark.ml import PipelineModel
from pyspark.sql.functions import udf

INPUT_PATH = "<...>"
MODEL_PATH = "<...>/MODEL"
OUTPUT_PATH = "<...>"
timestampformat = "DDD MMM dd HH:mm:ss zzzz yyyy"

spark = SparkSession.builder.appName("StreamingSentiment").getOrCreate()
schema = spark.read.json(INPUT_PATH).limit(10).schema

#Using ML Persistence, we simply load the pre-trained model that we stored from before
sentiment_model = PipelineModel.load(MODEL_PATH)

#Select between DataStreamReader or DataReader instances
#static spark reader
spark_reader = spark.read.schema(schema)
#streaming spark reader
# spark_reader = spark.readStream.schema(schema)


@udf
def html_unescape(s: str):
    if isinstance(s, str):
        return html.unescape(s)
    return s


def clean_data(df: DataFrame):
    url_regex = r"((https?|ftp|file):\/{2,3})+([-\w+&@#/%=~|$?!:,.]*)|(www.)+([-\w+&@#/%=~|$?!:,.]*)"
    email_regex = r"[\w.-]+@[\w.-]+\.[a-zA-Z]{1,}"
    user_regex = r"(@\w{1,15})"

    return (
        df

        .withColumn("original_text", f.col("text"))

        .withColumn("text", f.regexp_replace(f.col("text"), url_regex, " "))
        .withColumn("text", f.regexp_replace(f.col("text"), email_regex, " "))
        .withColumn("text", f.regexp_replace(f.col("text"), user_regex, " "))
        .withColumn("text", f.regexp_replace(f.col("text"), "#", " "))

        .withColumn("text", html_unescape(f.col("text")))

        .withColumn("text", f.regexp_replace(f.col("text"), "[^a-zA-Z']", " "))
        .withColumn("text", f.regexp_replace(f.col("text"), " +", " "))
        .withColumn("text", f.trim(f.col("text")))

        .filter(f.col("text") != "").na.drop(subset="text")
    )


data_in = clean_data(
    spark_reader.json(INPUT_PATH)
    .select(
        f.col("data.id").alias("id"),
        f.col("data.text").alias("text"),
        f.col("matching_rules.tag").alias("tag")
    )
    .coalesce(1)
)

if not data_in.isStreaming:
    data_in.limit(10).show()

raw_sentiment = sentiment_model.transform(data_in)

#Select downstream columns
sentiment = raw_sentiment.select(
    "id", "text", "tag", f.col("prediction").alias("user_sentiment")
)

#queries
negative_sentiment_count = (
    sentiment.filter("user_sentiment == 0.0")
    .select(f.col("user_sentiment").alias("negative_sentiment"))
    .agg(f.count("negative_sentiment"))
)

positive_sentiment_count = (
    sentiment.filter("user_sentiment == 4.0")
    .select(f.col("user_sentiment").alias("positive_sentiment"))
    .agg(f.count("positive_sentiment"))
)

average_sentiment = sentiment.agg(f.avg("user_sentiment"))

# data_to_stream = negative_sentiment_count

# data_to_stream = positive_sentiment_count

data_to_stream = average_sentiment

# Start streaming

if isinstance(spark_reader, DataStreamReader):
    stream_writer = (
        data_to_stream.writeStream.queryName("streaming_table")
        .trigger(processingTime="14 seconds")
        .outputMode("complete")
        .format("memory")
    )

    query = stream_writer.start()

if data_in.isStreaming:
    print(str(query.lastProgress))

if data_in.isStreaming:
    from time import sleep
    for x in range(0, 200):
        try:
            if not query.isActive:
                break
            print("Showing live view refreshed every 7 seconds")
            print(f"Seconds passed: {x*7}")
            result = spark.sql(f"SELECT * from {query.name}")
            result.show()
            sleep(5)
        except KeyboardInterrupt:
            break
    print("Live view ended. . .")
else:
    print("Not streaming, showing static output instead")
    result = data_to_stream
    result.limit(10).show()
    sentiment.write.json(OUTPUT_PATH)