from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, lit, sin, cos, when, expr, array
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.functions import vector_to_array, array_to_vector
import math
from pyspark.sql.functions import udf, abs as sql_abs
from pyspark.sql.types import StringType

# Spark Session
spark = SparkSession.builder \
    .appName("MusicRecommenderConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()
print(spark.version)

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


# Convertir de binario a string y parsear JSON
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

print("Modelos cargados correctamente.")
# 6. Cargar canciones clusterizadas
clustered_songs = spark.read.csv(
    "/app/data/songs_with_cluster.csv", header=True, inferSchema=True)

print("Canciones clusterizadas cargadas correctamente.")

print("antes del batch")
# 7. Proceso por batch
def process_batch(batch_df, batch_id):
    try:
        print(f"\n--- Batch {batch_id} ---")
        batch_df.printSchema()
        batch_df.show()
        print("ENTRO al batch")

        if batch_df.count() > 0:
            print("Procesando batch...")
            # A. Preprocesamiento de columnas (igual que en entrenamiento)
            df_trans = batch_df \
                .filter((col("tempo") >= 40) & (col("tempo") <= 250)) \
                .filter((col("time_signature") >= 1) & (col("time_signature") <= 5)) \
                .withColumn("key_sin", sin(2 * math.pi * col("key") / 12)) \
                .withColumn("key_cos", cos(2 * math.pi * col("key") / 12)) \
                .withColumn("loudness_pos", col("loudness") + 60) \
                .withColumn("mode_major", when(col("mode") == 1, 1).otherwise(0))

            print("Preprocesamiento completado.")
            # B. Aplicar ensamblado y escalado
            assembled = assembler.transform(df_trans)
            scaled = scaler_model.transform(assembled)

            print("Ensamblado y escalado completados.")


            # C. Aplicar pesos
            # C. Aplicar pesos con PySpark puro
            weights = [4.5, 1.0, 5.0, 5.0, 0.5, 0.5]

            # Convertir el vector en array
            scaled = scaled.withColumn(
                "scaled_array", vector_to_array(col("scaled_features")))

            # Multiplicar cada componente por su peso
            weighted_cols = [col("scaled_array")[i] * lit(w)
                            for i, w in enumerate(weights)]

            # Reconvertir a vector
            weighted = scaled.withColumn(
                "weighted_scaled_features", array_to_vector(array(*weighted_cols)))
            print("Pesos aplicados.")
            # D. Predecir con KMeans
            predictions = kmeans_model.transform(weighted)
            predictions.select("track_name", "cluster_id").show()

            predicted_cluster = predictions.select(
                "cluster_id").first()["cluster_id"]
            recommendation_id = batch_df.select("recommendation_id").first()[
                "recommendation_id"]

            print("Predicción de cluster realizada.")
            # E. Buscar canciones similares
            recommendations = clustered_songs.filter(
                col("cluster_id") == predicted_cluster
            ).select("artist", "title").limit(3)

            recommendations = recommendations.withColumn(
                "recommendation_id", lit(recommendation_id))
            ###################################################################### BLOQUE CAMELOT
            # 1. UDF para convertir a Camelot
            def to_camelot(key, mode):
                key = int(key)
                mode = int(mode)
                major_map = {
                    0: "8B", 1: "3B", 2: "10B", 3: "5B", 4: "12B", 5: "7B",
                    6: "2B", 7: "9B", 8: "4B", 9: "11B", 10: "6B", 11: "1B"
                }
                minor_map = {
                    0: "5A", 1: "12A", 2: "7A", 3: "2A", 4: "9A", 5: "4A",
                    6: "11A", 7: "6A", 8: "1A", 9: "8A", 10: "3A", 11: "10A"
                }
                return major_map.get(key) if mode == 1 else minor_map.get(key)


            camelot_udf = udf(to_camelot, StringType())

            # 2. Agregar camelot al input y a las canciones del cluster
            predictions = predictions.withColumn("camelot", camelot_udf("key", "mode"))
            input_row = predictions.select("tempo", "camelot").first()
            input_tempo = input_row["tempo"]
            input_camelot = input_row["camelot"]

            # 3. Agregar camelot a las canciones clusterizadas
            songs_in_cluster = clustered_songs.filter(
                col("cluster_id") == predicted_cluster
            ).withColumn("camelot", camelot_udf("key", "mode"))

            # 4. Define vecinos válidos Camelot
            def camelot_neighbors(camelot_key):
                num = int(camelot_key[:-1])
                scale = camelot_key[-1]
                neighbors = [camelot_key]
                neighbors.append(f"{num - 1 if num > 1 else 12}{scale}")
                neighbors.append(f"{num + 1 if num < 12 else 1}{scale}")
                neighbors.append(f"{num}{'B' if scale == 'A' else 'A'}")  # relative
                return neighbors


            valid_camelot = camelot_neighbors(input_camelot)

            # 5. Filtrar por key compatible y tempo más cercano
            filtered = songs_in_cluster.filter(col("camelot").isin(valid_camelot)) \
                .withColumn("tempo_diff", sql_abs(col("tempo") - lit(input_tempo))) \
                .orderBy("tempo_diff") \
                .limit(1)

            recommendations = filtered.withColumn("recommendation_id", lit(recommendation_id)) \
                .select("artist", "title", "tempo", "key", "recommendation_id")
            ################################################################################################

            print("Canciones similares encontradas.")
            print("comienzo a enviar a Kafka")
            # F. Enviar a Kafka
            kafka_ready = recommendations \
                .withColumn("value", to_json(struct("artist", "title","tempo","key"))) \
                .withColumn("key", col("recommendation_id")) \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

            kafka_ready.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "broker:29092") \
                .option("topic", "music-recommendation-result") \
                .save()
            print("Recomendaciones enviadas a Kafka.")

            print("\nRecomendaciones enviadas:")
            recommendations.show()

    except Exception as e:
        print(f"[ERROR en batch {batch_id}]: {e}")


# 8. Iniciar stream
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
