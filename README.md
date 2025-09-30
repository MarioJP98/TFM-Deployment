# TFM-Deployment

Este repositorio contiene el código, notebooks y configuración necesarios relacionados con el **Trabajo Fin de Máster (TFM) de Mario Jiménez**.

---

## 🎵 Data Sources

Este proyecto utiliza [GetSongBPM](https://getsongbpm.com) como fuente de información de **tempo** y **tonalidad** de las canciones.

---

## 📂 Estructura del repositorio

- **`data_analysis-test/`**  
  Contiene scripts en **Python** junto con los primeros **tests** realizados para el **preprocesamiento** y **análisis exploratorio** del dataset.

- **`notebooks/`**  
  Carpeta con **Jupyter Notebooks** usados durante el desarrollo y experimentación del proyecto. Incluye:
  - **K-Means/** → Código utilizado para el modelo de clustering.
  - **MLP/** → Código utilizado para el modelo de redes neuronales *Multilayer Perceptron*.
  - **EDA/** → Archivos en **HTML** con el análisis exploratorio de datos (*Exploratory Data Analysis*) y construcción del dataset.

- **`docker/`**  
  Archivos de configuración necesarios para el **despliegue con Docker**.  
  Cada subcarpeta está asociada al despliegue de un componente específico del proyecto.  
  Se incluyen la definición de imágenes, instalación de dependencias y variables de entorno necesarias.

---

# 🚀 Despliegue

El repositorio presenta 2 arquitecturas que se despliegan de manera independiente:  

- **Desarrollo** → entorno de experimentación con Spark y Jupyter.  
- **Producción** → arquitectura completa con Django, PostgreSQL, Kafka y Spark.  

---

## 🔧 Entorno de Desarrollo

El entorno de desarrollo utiliza **Docker Compose** para levantar un clúster de **Apache Spark** junto con un servidor de **JupyterLab** conectado al clúster.

### 1️⃣ Requisitos previos

- [Docker](https://docs.docker.com/get-docker/)  
- [Docker Compose](https://docs.docker.com/compose/)  

### 2️⃣ Levantar los servicios

En /docker/dev/, ejecuta:

```
docker compose -f docker/dev/docker-compose.yml up -d
```

Esto desplegará los siguientes contenedores:

- **spark-master** → Nodo maestro de Apache Spark (puertos `7077`, `8080`)  
- **spark-worker** → Nodo trabajador conectado al maestro (usa 8GB RAM y 2 cores, configurable en `docker-compose.yml`)  
- **jupyter-spark** → Entorno JupyterLab (puerto `8888`) con acceso directo al clúster Spark.  

### 3️⃣ Acceso a los servicios

- **Interfaz web de Spark Master:** [http://localhost:8080](http://localhost:8080)  
- **JupyterLab:** [http://localhost:8888](http://localhost:8888)  

⚡ El JupyterLab se lanza **sin token de acceso** (configurado en el `docker-compose.yml`).

### 4️⃣ Volúmenes montados

- `./notebooks` → Se monta en `/app/notebooks` dentro del contenedor Jupyter.  
- `./dataset` → Disponible en Spark en `/opt/bitnami/spark/datasets`.  
- `./models` → Carpeta compartida entre todos los contenedores para guardar modelos entrenados.  

### 5️⃣ Apagar los servicios

```
docker compose down
```

Esto detendrá y eliminará los contenedores (pero no los volúmenes persistentes).

---

## 🌐 Entorno de Producción

El entorno de producción integra **Django + PostgreSQL + Kafka + Spark** para desplegar el sistema completo de recomendación musical.  

### 1️⃣ Servicios principales

- **db (PostgreSQL)** → Base de datos relacional para la app Django.  
- **django-music-recommender** → Aplicación Django que expone la API web.  
- **broker (Kafka)** → Broker de mensajería para el procesamiento en streaming.  
- **init-kafka** → Inicializa los *topics* necesarios en Kafka.  
- **kafka-ui** → Interfaz gráfica para monitorizar Kafka en [http://localhost:8085](http://localhost:8085).  
- **spark-master / spark-worker** → Clúster de Apache Spark.  
- **spark-recommender-consumer** → Servicio Spark que consume mensajes de Kafka y genera recomendaciones.  

### 2️⃣ Variables de entorno

Todas las variables sensibles (DB, Django, etc.) se gestionan desde el archivo:  

```
./django/.env
```

Ejemplo de contenido:

```
DJANGO_SECRET_KEY=tu_secret_key
DEBUG=False
DJANGO_LOGLEVEL=info
DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1
DATABASE_ENGINE=django.db.backends.postgresql
DATABASE_NAME=tfm_db
DATABASE_USERNAME=tfm_user
DATABASE_PASSWORD=tfm_password
DATABASE_HOST=db
DATABASE_PORT=5432
```

### 3️⃣ Levantar los servicios

```
docker compose -f docker/prod/docker-compose.yml up -d
```

### 4️⃣ Acceso a los servicios

- **Django API:** [http://localhost:8000](http://localhost:8000)  
- **PostgreSQL:** `localhost:5432` (usuario/contraseña configurados en `.env`)  
- **Kafka UI:** [http://localhost:8085](http://localhost:8085)  
- **Spark Master:** [http://localhost:8080](http://localhost:8080)  

### 5️⃣ Apagar los servicios

```
docker compose -f docker-compose.prod.yml down
```

Esto detendrá y eliminará los contenedores, manteniendo los volúmenes de datos (Postgres, Kafka logs, etc.).

---

## 📄 Licencia

Este proyecto está bajo la licencia MIT.
