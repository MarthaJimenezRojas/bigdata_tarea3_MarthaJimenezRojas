# ================================================
# Tarea 3 - Procesamiento Batch con Spark
# Por: Martha Y. Jimenez Rojas
# Análisis del dataset de Seguridad Alimentaria - Casanare
# ================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# 1️⃣ Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Tarea3_Batch_SeguridadAlimentaria") \
    .getOrCreate()

# 2️⃣ Cargar el dataset desde HDFS
df = spark.read.csv("hdfs:///batchTarea3/seguridad_alimentaria.csv", header=True, inferSchema=True)

print("=== Esquema detectado ===")
df.printSchema()

print("=== Primeras filas ===")
df.show(5)

# 3️⃣ Limpieza básica de datos
columnas_clave = ["ITEM", "NOMBRES ", "APELLIDOS", "CÓDIGO DEL DEPARTAMENTO"]
df_clean = df.na.drop(subset=columnas_clave)

# 4️⃣ Análisis básico
print("=== Estadísticas generales ===")
df_clean.describe().show()

print("=== Registros por municipio (Top 10) ===")
df_clean.groupBy("MUNICIPIO ").count().orderBy(col("count").desc()).show(10)

print("=== Registros por código de departamento (Top 10) ===")
df_clean.groupBy("CÓDIGO DEL DEPARTAMENTO").count().orderBy(col("count").desc()).show(10)

print("=== Promedio del valor de 'FECHA DEL BENEFICIO Y/O INTERES' por municipio (si aplica) ===")
df_clean.groupBy("MUNICIPIO ").agg(avg("FECHA DEL BENEFICIO Y/O INTERES").alias("Promedio_Fecha")).orderBy(col("Promedio_Fecha").desc()).show(10)

# =======================================================
# 🆕 BLOQUE NUEVO: Análisis adicional (más profundo)
# =======================================================

print("\n=== Distribución de beneficiarios por género ===")
df_genero = df_clean.groupBy("GÉNERO").count().orderBy(col("count").desc())
df_genero.show(10)

print("\n=== Tipo de beneficio más común ===")
df_beneficio = df_clean.groupBy('"BENEFICIO Y/O INTERES CATEGORIA"').count().orderBy(col("count").desc())
df_beneficio.show(10)

print("\n=== Bien o servicio más entregado ===")
df_servicio = df_clean.groupBy('"BENEFICIO Y/O INTERES BIEN Y/O SERVICIO"').count().orderBy(col("count").desc())
df_servicio.show(10)

print("\n=== Programas más frecuentes ===")
df_programa = df_clean.groupBy("PROGRAMA").count().orderBy(col("count").desc())
df_programa.show(10)

print("\n=== Cruce: número de beneficiarios por municipio y género ===")
df_mun_genero = df_clean.groupBy("MUNICIPIO ", "GÉNERO").count().orderBy(col("count").desc())
df_mun_genero.show(15)

# 💾 Guardar nuevos resultados en subcarpetas
df_genero.coalesce(1).write.mode("overwrite").csv("/batchTarea3/resultados/genero", header=True)
df_beneficio.coalesce(1).write.mode("overwrite").csv("/batchTarea3/resultados/beneficios", header=True)
df_servicio.coalesce(1).write.mode("overwrite").csv("/_
