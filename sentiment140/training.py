from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.ml.feature import (
    StopWordsRemover,
    Tokenizer,
    HashingTF,
    IDF,
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = (
    SparkSession.builder.appName("ModelTraining")
    .config("spark.executor.memory", "1g")
    .getOrCreate()
)

schema = "polarity FLOAT, id LONG, date_time TIMESTAMP, query STRING, user STRING, text STRING"

INPUT_PATH = "<...>/CLEAN"
OUTPUT_PATH = "<...>/MODEL"

spark_reader = spark.read.schema(schema)

df = spark_reader.parquet(INPUT_PATH)

df = (
    df
    # Remove all numbers
    .withColumn("text", f.regexp_replace(f.col("text"), "[^a-zA-Z]", " "))
    # Remove all double/multiple spaces
    .withColumn("text", f.regexp_replace(f.col("text"), " +", " "))
    # Remove leading and trailing whitespaces
    .withColumn("text", f.trim(f.col("text")))
    # Ensure we don't end up with empty rows
    .filter("text != ''")
)

data = df.select("text", "polarity").coalesce(3).cache()

(training_data, validation_data, test_data) = data.randomSplit([0.98, 0.01, 0.01], seed=2021)

tokenizer = Tokenizer(inputCol="text", outputCol="words1")
stopwords_remover = StopWordsRemover(
    inputCol="words1",
    outputCol="word2",
    stopWords=StopWordsRemover.loadDefaultStopWords("english")
)
hashing_tf = HashingTF(
    inputCol="word2",
    outputCol="term_frequency",
)
idf = IDF(
    inputCol="term_frequency",
    outputCol="features",
    minDocFreq=5,
)
lr = LogisticRegression(labelCol="polarity")

semantic_analysis_pipeline = Pipeline(
    stages=[tokenizer, stopwords_remover, hashing_tf, idf, lr]
)

semantic_analysis_model = semantic_analysis_pipeline.fit(training_data)

trained_df = semantic_analysis_model.transform(training_data)
val_df = semantic_analysis_model.transform(validation_data)
test_df = semantic_analysis_model.transform(test_data)

evaluator = MulticlassClassificationEvaluator(labelCol="polarity", metricName="accuracy")

accuracy_val = evaluator.evaluate(val_df)
accuracy_test = evaluator.evaluate(test_df)
print("wwwwwwwwwwwwwwwwwwwwwwwwwwwww Validation Data:")
print(f"Accuracy: {accuracy_val}%")
print("wwwwwwwwwwwwwwwwwwwwwwwwwwwww Testing Data:")
print(f"Accuracy: {accuracy_test}%")

final_model = semantic_analysis_model
final_model.save(OUTPUT_PATH)

