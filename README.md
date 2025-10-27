# Proyecto Big Data - Seguridad Alimentaria Casanare

Autora: Martha Jimenez Rojas
Curso: Big Data - UNAD 2025  
Herramientas:Apache Hadoop, Apache Spark, Apache Kafka, Python  


## Descripción general

Este proyecto analiza el conjunto de datos **“Seguridad Alimentaria - Casanare”**, disponible en Datos Abiertos de Colombia.  
El objetivo es identificar y analizar la población beneficiaria de programas de seguridad alimentaria en el departamento, diferenciando por municipio, género, tipo de beneficio y programa.  

La solución se divide en dos componentes principales:

1. Procesamiento Batch (por lotes) con Apache Spark.  
2. Procesamiento en Tiempo Real (Streaming) con Apache Spark Streaming y Apache Kafka.


##  Dataset utilizado

Fuente (Copair y Pegar el enlace para poder acceder) : https://www.datos.gov.co/Inclusi-n-Social-y-Reconciliaci-n/Seguridad-Alimentaria/c7vi-t4i8/about_data

Descripción oficial:
> Mejorar la seguridad alimentaria en el Departamento de Casanare mediante la entrega de paquetes nutricionales, huertas de autoconsumo y formación en estilos de vida saludables.  
> Incluye hogares con niños, madres gestantes, adultos mayores y personas con discapacidad de zonas rurales y urbanas.

Ubicación del archivo en HDFS:
`/batchTarea3/seguridad_alimentaria.csv`

---

## Requisitos previos

Antes de ejecutar el proyecto se debe  tener:

- Ubuntu Linux (entorno virtual o físico).  
- Apache Hadoop configurado y en ejecución (`start-all.sh`).  
- Apache Spark 3.x instalado.  
- Apache Kafka 3.x instalado en `/opt/Kafka`.  
- Python 3.10+ con la librería `kafka-python`.

Instalar el paquete necesario:
```bash
pip3 install kafka-python
