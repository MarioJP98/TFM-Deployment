from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, lit, sin, cos, when, expr
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.functions import vector_to_array, array_to_vector
import math

# 1. Crear sesión Spark
spark = SparkSession.builder \
    .appName("MusicRecommenderConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

# 2. Esquema esperado (solo columnas necesarias para el modelo)
schema = StructType([
    StructField("track_name", StringType()),
    StructField("artist_name", StringType()),
    StructField("tempo", DoubleType()),
    StructField("loudness", DoubleType()),
    StructField("key", DoubleType()),
    StructField("mode", DoubleType()),
    StructField("time_signature", DoubleType()),
])

# 3. Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "music-recommendation-features") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsear JSON
df = df.selectExpr("CAST(key AS STRING) as recommendation_id",
                   "CAST(value AS STRING) as json_str")
parsed_df = df.select(
    from_json(col("json_str"), schema).alias("data"),
    col("recommendation_id")
).select("data.*", "recommendation_id")

# 5. Cargar modelos
assembler = VectorAssembler.load(
    "/app/models/music-recommender-model/assembler")
scaler_model = StandardScalerModel.load(
    "/app/models/music-recommender-model/scaler")
kmeans_model = KMeansModel.load("/app/models/music-recommender-model/kmeans")

# 6. Cargar canciones clusterizadas
clustered_songs = spark.read.csv(
    "/app/data/songs_with_cluster.csv", header=True, inferSchema=True)

# 7. Proceso por batch


def process_batch(batch_df, batch_id):
    try:
        print(f"\n--- Batch {batch_id} ---")

        if batch_df.count() > 0:
            # A. Preprocesamiento de columnas (igual que en entrenamiento)
            df_trans = batch_df \
                .filter((col("tempo") >= 40) & (col("tempo") <= 250)) \
                .filter((col("time_signature") >= 1) & (col("time_signature") <= 5)) \
                .withColumn("key_sin", sin(2 * math.pi * col("key") / 12)) \
                .withColumn("key_cos", cos(2 * math.pi * col("key") / 12)) \
                .withColumn("loudness_pos", col("loudness") + 60) \
                .withColumn("mode_major", when(col("mode") == 1, 1).otherwise(0))

            # B. Aplicar ensamblado y escalado
            assembled = assembler.transform(df_trans)
            scaled = scaler_model.transform(assembled)

            # C. Aplicar pesos
            weights = [4.5, 1.0, 5.0, 5.0, 0.5, 0.5]
            weighted_expr = f"transform(vector_to_array(scaled_features), (x, i) -> x * array({','.join(map(str, weights))})[i])"
            weighted = scaled.withColumn(
                "weighted_scaled_features", array_to_vector(expr(weighted_expr)))

            # D. Predecir con KMeans
            predictions = kmeans_model.transform(weighted)
            predictions.select("track_name", "cluster_id").show()

            predicted_cluster = predictions.select(
                "cluster_id").first()["cluster_id"]
            recommendation_id = batch_df.select("recommendation_id").first()[
                "recommendation_id"]

            # E. Buscar canciones similares
            recommendations = clustered_songs.filter(
                col("cluster_id") == predicted_cluster
            ).select("artist", "title").limit(3)

            recommendations = recommendations.withColumn(
                "recommendation_id", lit(recommendation_id))

            # F. Enviar a Kafka
            kafka_ready = recommendations \
                .withColumn("value", to_json(struct("artist", "title"))) \
                .withColumn("key", col("recommendation_id")) \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

            kafka_ready.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "broker:29092") \
                .option("topic", "music-recommendation-result") \
                .save()

            print("\nRecomendaciones enviadas:")
            recommendations.show()

    except Exception as e:
        print(f"[ERROR en batch {batch_id}]: {e}")


# 8. Iniciar stream
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
