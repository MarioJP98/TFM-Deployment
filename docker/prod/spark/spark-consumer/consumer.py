from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_json, struct, lit, sin, cos, when, array
)
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.functions import vector_to_array, array_to_vector

import math
import joblib
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# ---------- Config ----------
TOP_C = 2          # nº de clusters a expandir
TOP_N = 200        # nº de candidatos iniciales tras coseno
K_FINAL = 50       # tamaño final tras MMR
LAMBDA_REL = 0.7   # trade-off relevancia/diversidad

# ---------- Utilidades ----------

camelot_major = {0: 8, 7: 9, 2: 10, 9: 11, 4: 12, 11: 1,
                 6: 2, 1: 3, 8: 4, 3: 5, 10: 6, 5: 7}
camelot_minor = {9: 8, 4: 9, 11: 10, 6: 11, 1: 12, 8: 1,
                 3: 2, 10: 3, 5: 4, 0: 5, 7: 6, 2: 7}





def feature_columns(df: pd.DataFrame):
    """Detecta columnas f_0..f_n en orden correcto."""
    return [c for c in df.columns if c.startswith("f_")]


def cosine_sim_matrix(seed_vec: np.ndarray, M: np.ndarray) -> np.ndarray:
    seed_norm = np.linalg.norm(seed_vec)
    M_norms = np.linalg.norm(M, axis=1)
    denom = (seed_norm * M_norms)
    denom[denom == 0] = 1e-12
    return (M @ seed_vec) / denom


def mmr_select(seed_vec: np.ndarray, cand_mat: np.ndarray, items_idx: np.ndarray, k=50, lambda_rel=0.7):
    """MMR greedy: combina relevancia (cosine con seed) y diversidad."""
    selected = []
    selected_idx = []
    if len(items_idx) == 0:
        return selected_idx

    rel = cosine_sim_matrix(seed_vec, cand_mat)

    # más relevante primero
    best0 = int(np.argmax(rel))
    selected_idx.append(items_idx[best0])
    selected.append(cand_mat[best0])

    # iterativamente añade con MMR
    while len(selected_idx) < min(k, len(items_idx)):
        best_i, best_score = None, -1e9
        for i in range(len(items_idx)):
            if items_idx[i] in selected_idx:
                continue
            max_sim = 0.0
            for s in selected:
                denom = (np.linalg.norm(cand_mat[i]) * np.linalg.norm(s))
                sim = 0.0 if denom == 0 else float(
                    np.dot(cand_mat[i], s) / denom)
                if sim > max_sim:
                    max_sim = sim
            score = lambda_rel * rel[i] - (1 - lambda_rel) * max_sim
            if score > best_score:
                best_score = score
                best_i = i
        selected_idx.append(items_idx[best_i])
        selected.append(cand_mat[best_i])
    return selected_idx


# ---------- Spark Session ----------
spark = SparkSession.builder \
    .appName("MusicRecommenderConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()


@F.udf("int")
def camelot_number_udf(key, mode):
    if key is None or mode is None:
        return None
    return camelot_major.get(int(key)) if int(mode) == 1 else camelot_minor.get(int(key))


@F.udf("string")
def camelot_letter_udf(mode):
    return "B" if int(mode) == 1 else "A"

# ---------- Esquema esperado ----------
schema = StructType([
    StructField("track_name", StringType()),
    StructField("artist_name", StringType()),
    StructField("tempo", DoubleType()),
    StructField("loudness", DoubleType()),
    StructField("key", DoubleType()),
    StructField("mode", DoubleType()),
    StructField("time_signature", DoubleType()),
])

# ---------- Kafka input ----------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "music-recommendation-features") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(key AS STRING) as recommendation_id",
                   "CAST(value AS STRING) as json_str")

parsed_df = df.select(
    from_json(col("json_str"), schema).alias("data"),
    col("recommendation_id")
).select("data.*", "recommendation_id")

# ---------- Cargar modelos ----------
assembler = VectorAssembler.load(
    "/app/models/music-recommender-model/assembler")
scaler_model = StandardScalerModel.load(
    "/app/models/music-recommender-model/scaler")
kmeans_model = KMeansModel.load("/app/models/music-recommender-model/kmeans")

mlp = joblib.load("/app/models/music-recommender-model/mlp_camelot_adam.pkl")
catalog = pd.read_parquet("/app/data/v8_mlp.parquet")
feat_cols = feature_columns(catalog)

print("Modelos cargados correctamente.")


# ---------- Proceso por batch ----------
def process_batch(batch_df, batch_id):
    try:
        print(f"\n--- Batch {batch_id} ---")
        if batch_df.count() == 0:
            return

        # A. Preprocesamiento Spark (igual que en entrenamiento)
        df_trans = batch_df \
            .filter((col("tempo") >= 40) & (col("tempo") <= 250)) \
            .filter((col("time_signature") >= 1) & (col("time_signature") <= 5)) \
            .withColumn("loudness_pos", col("loudness") + 60)

        # Añadir columnas Camelot
        df_trans = df_trans.withColumn(
            "camelot_num", camelot_number_udf(col("key"), col("mode")))
        df_trans = df_trans.withColumn(
            "camelot_letter", camelot_letter_udf(col("mode")))

        angle = (2 * math.pi) * (col("camelot_num") - F.lit(1)) / F.lit(12)
        df_trans = df_trans.withColumn("camelot_cos", cos(angle))
        df_trans = df_trans.withColumn("camelot_sin", sin(angle))
        df_trans = df_trans.withColumn(
            "camelot_mode_small",
            F.when(col("camelot_letter") == F.lit("B"),
                   F.lit(1.0)).otherwise(F.lit(0.0))
        )

        # Ensamblar y escalar
        assembled = assembler.transform(df_trans)
        scaled = scaler_model.transform(assembled)

        # aplicar pesos
        weights = [4.5, 1.0, 5.0, 5.0, 0.5, 0.5]
        scaled = scaled.withColumn(
            "scaled_array", vector_to_array(col("scaled_features")))
        weighted_cols = [col("scaled_array")[i] * lit(w)
                         for i, w in enumerate(weights)]
        weighted = scaled.withColumn(
            "weighted_scaled_features", array_to_vector(array(*weighted_cols)))

        # B. KMeans
        predictions = kmeans_model.transform(weighted)

        # C. Extraer vector semilla desde weighted_scaled_features
        preds_seed = predictions.select(
            vector_to_array(col("weighted_scaled_features")).alias("feat_arr"),
            col("recommendation_id")
        ).toPandas()

        if preds_seed.empty:
            return

        seed_vec = np.array(preds_seed.loc[0, "feat_arr"], dtype=np.float32)
        recommendation_id = preds_seed.loc[0, "recommendation_id"]

        # D. MLP
        probs = mlp.predict_proba(seed_vec.reshape(1, -1))[0]
        topC_clusters = np.argsort(probs)[::-1][:TOP_C]

        # E. Selección candidatos
        candidates = catalog[catalog["label"].isin(topC_clusters)].copy()
        M = candidates[feat_cols].to_numpy(dtype=np.float32)
        sims = cosine_sim_matrix(seed_vec, M)
        order = np.argsort(sims)[::-1]
        candidates_topN = candidates.iloc[order[:TOP_N]].copy()

        # F. Diversificación MMR
        items_idx = candidates_topN.index.to_numpy()
        cand_mat = candidates_topN[feat_cols].to_numpy(dtype=np.float32)
        mmr_indices = mmr_select(
            seed_vec, cand_mat, items_idx, k=K_FINAL, lambda_rel=LAMBDA_REL)
        playlist = catalog.loc[mmr_indices].copy()

        # G. Enviar a Kafka
        cols_to_send = [c for c in ["artist", "title"]
                        if c in playlist.columns]
        recs_spark = spark.createDataFrame(playlist[cols_to_send]) \
            .withColumn("recommendation_id", lit(recommendation_id))

        kafka_ready = recs_spark.withColumn("value", to_json(struct(*cols_to_send))) \
            .withColumn("key", col("recommendation_id")) \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        kafka_ready.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("topic", "music-recommendation-result") \
            .save()

        print(f"Recomendaciones enviadas para {recommendation_id}")
        print(playlist[cols_to_send].head(10))

    except Exception as e:
        print(f"[ERROR en batch {batch_id}]: {e}")


# ---------- Iniciar stream ----------
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
