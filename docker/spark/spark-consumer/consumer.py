from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml import PipelineModel
from pyspark.sql.functions import lit


# Crear sesión Spark
spark = SparkSession.builder \
    .appName("MusicRecommenderConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

# Esquema esperado del JSON entrante
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

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "music-recommendation-features") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir de binario a string y parsear JSON
df = df.selectExpr("CAST(key AS STRING) as recommendation_id", "CAST(value AS STRING) as json_str")
parsed_df = df.select(
    from_json(col("json_str"), schema).alias("data"),
    col("recommendation_id")
).select("data.*", "recommendation_id")


# Cargar modelo entrenado
model = PipelineModel.load("/app/models/music-recommender-model")

# Cargar canciones ya clusterizadas desde CSV
clustered_songs = spark.read.csv(
    "/app/data/songs_with_cluster.csv", header=True, inferSchema=True)

# Función de inferencia por batch y recomendación
def process_batch(batch_df, batch_id):
    try:
        print(f"\n--- Batch {batch_id} ---")
        batch_df.printSchema()
        batch_df.show()

        if batch_df.count() > 0:
            predictions = model.transform(batch_df)
            predictions.select("track_name", "cluster_id").show()

            predicted_cluster = predictions.select("cluster_id").first()["cluster_id"]
            recommendation_id = batch_df.select("recommendation_id").first()["recommendation_id"]


            # Buscar canciones similares
            recommendations = clustered_songs.filter(
                col("cluster_id") == predicted_cluster
            ).select("artist", "title").limit(3)

            recommendations = recommendations.withColumn("recommendation_id", lit(recommendation_id))


            # Mostrar por consola
            print("\Recomendaciones similares:")
            recommendations.show()

            # Enviar recomendaciones a Kafka
            kafka_ready = recommendations \
                .withColumn("value", to_json(struct("artist", "title"))) \
                .withColumn("key", col("recommendation_id")) \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

            kafka_ready.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "broker:29092") \
                .option("topic", "music-recommendation-result") \
                .save()

    except Exception as e:
        print(f"[ERROR en batch {batch_id}]: {e}")

# Iniciar el stream
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
