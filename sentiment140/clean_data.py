
import html

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

schema = "polarity FLOAT, id LONG, date_time TIMESTAMP, query STRING, user STRING, text STRING"

timestampformat = "DDD MMM dd HH:mm:ss zzz yyyy"

INPUT_PATH = "<...>/RAW"

OUTPUT_DIR = "<...>/CLEAN"

spark_reader = spark.read.schema(schema)


@f.udf()
def html_unescape(s: str):
    if isinstance(s, str):
        return html.unescape(s)
    return s

user_regex = r"(@\w{1,15})"
hashtag_regex = r"(#\w{1,})"
url_regex = r"((https?|ftp|file):\/{2,3})+([-\w+&@#/%=~|$?!:,.]*)|(www.)+([-\w+&@#/%=~|$?!:,.]*)"
email_regex = r"[\w.-]+@[\w.-]+\.[a-zA-Z]{1,}"


def clean_data(df):
    df = (
        df
        .withColumn("original_text", f.col("text"))
        .withColumn("text", f.regexp_replace(f.col("text"), url_regex, ""))
        .withColumn("text", f.regexp_replace(f.col("text"), email_regex, ""))
        .withColumn("text", f.regexp_replace(f.col("text"), user_regex, ""))
        .withColumn("text", f.regexp_replace(f.col("text"), hashtag_regex, ""))
        .withColumn("text", html_unescape(f.col("text")))
        .filter("text != ''")
    )
    return df


df_raw = spark_reader.csv(INPUT_PATH, timestampFormat=timestampformat).cache()
df_clean = clean_data(df_raw)

df_clean.write.partitionBy("polarity").parquet(OUTPUT_DIR)

