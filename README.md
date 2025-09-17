## Data Sources

This project uses [GetSongBPM](https://getsongbpm.com) as a source of song tempo and key information.


# TFM-Deployment

Este repositorio contiene el código, notebooks y configuración necesarios relacionados con el **Trabajo Fin de Máster (TFM) de Mario Jiménez**.


---

## Estructura del repositorio

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

- **`.gitignore`**
  Fichero de configuración de Git que indica qué archivos y directorios deben ignorarse en el control de versiones.
