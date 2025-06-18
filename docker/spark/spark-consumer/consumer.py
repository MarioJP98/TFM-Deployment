from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml import PipelineModel

# Spark Session
spark = SparkSession.builder \
    .appName("MusicRecommenderConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

# Esquema completo con timbre_mean y timbre_std
schema = StructType([
    StructField("track_name", StringType()),
    StructField("artist_name", StringType()),
    StructField("tempo", DoubleType()),
    StructField("loudness", DoubleType()),
    StructField("duration", DoubleType()),
    StructField("key", DoubleType()),
    StructField("mode", DoubleType()),
    StructField("time_signature", DoubleType()),
] + [
    StructField(f"timbre_mean_{i}", DoubleType()) for i in range(12)
] + [
    StructField(f"timbre_std_{i}", DoubleType()) for i in range(12)
])

# Leemos de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "music-recommendation-features") \
    .option("startingOffsets", "latest") \
    .load()

df.selectExpr("CAST(value AS STRING)").writeStream \
    .format("console") \
    .start()

# Convertimos de binario a String
df = df.selectExpr("CAST(value AS STRING) as json_str")

# Parseamos el JSON
parsed_df = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Cargamos el pipeline completo
model = PipelineModel.load("/app/models/music-recommender-model")

# Función de inferencia por batch con manejo de errores
def process_batch(batch_df, batch_id):
    try:
        print(f"\n--- Batch {batch_id} ---")
        batch_df.printSchema()
        batch_df.show()
        if batch_df.count() > 0:
            predictions = model.transform(batch_df)
            predictions.select("track_name", "cluster_id").show()
    except Exception as e:
        print(f"[ERROR en batch {batch_id}]: {e}")

# Ejecutamos el stream
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
