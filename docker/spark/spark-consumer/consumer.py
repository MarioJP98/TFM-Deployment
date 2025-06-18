from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml import PipelineModel

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
df = df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = df.select(
    from_json(col("json_str"), schema).alias("data")).select("data.*")

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

            predicted_cluster = predictions.select(
                "cluster_id").first()["cluster_id"]

            # Buscar canciones similares
            recommendations = clustered_songs.filter(
                col("cluster_id") == predicted_cluster)

            print("\n Recomendaciones similares:")
            recommendations.select("artist", "title").limit(3).show()
    except Exception as e:
        print(f"[ERROR en batch {batch_id}]: {e}")


# Iniciar el stream
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
