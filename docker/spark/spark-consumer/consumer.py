from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, StringType
from pyspark.ml import PipelineModel

# Spark Session
spark = SparkSession.builder \
    .appName("MusicRecommenderConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

# Esquema simplificado: solo las features que envía Django
schema = StructType() \
    .add("track_name", StringType()) \
    .add("artist_name", StringType()) \
    .add("tempo", DoubleType()) \
    .add("loudness", DoubleType()) \
    .add("duration", DoubleType()) \
    .add("key", DoubleType()) \
    .add("mode", DoubleType()) \
    .add("time_signature", DoubleType())

# Leemos de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "music-recommendation-features") \
    .option("startingOffsets", "latest") \
    .load()

# Convertimos de binario a String
df = df.selectExpr("CAST(value AS STRING) as json_str")

# Parseamos el JSON
parsed_df = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Cargamos el pipeline completo
model = PipelineModel.load("/app/models/music-recommender-model")

# Función de inferencia por batch
def process_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        predictions = model.transform(batch_df)
        predictions.select("track_name", "cluster_id").show()

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
