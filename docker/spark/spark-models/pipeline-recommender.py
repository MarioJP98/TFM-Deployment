from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler

# Features como en el notebook
feature_cols = ['tempo', 'loudness', 'duration', 'key', 'mode', 'time_signature'] + \
               [f'timbre_mean_{i}' for i in range(12)] + \
               [f'timbre_std_{i}' for i in range(12)]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_vec")
scaler = StandardScaler(inputCol="features_vec", outputCol="scaled_features")

# Ensamblamos el pipeline de preprocesado
preprocessing_pipeline = Pipeline(stages=[assembler, scaler])

# Ajustamos el pipeline
preprocessing_model = preprocessing_pipeline.fit(df)

# Transformamos los datos
df_scaled = preprocessing_model.transform(df)

# Entrenamos el KMeans con los datos escalados
from pyspark.ml.clustering import KMeans

kmeans = KMeans(featuresCol='scaled_features', predictionCol='cluster_id', k=20, seed=42)
kmeans_model = kmeans.fit(df_scaled)

# Finalmente guardamos el pipeline completo (preprocesado + modelo)
from pyspark.ml import PipelineModel

full_pipeline = Pipeline(stages=[assembler, scaler, kmeans])
full_pipeline_model = full_pipeline.fit(df)
full_pipeline_model.write().overwrite().save("spark-models/music-recommender-model")
