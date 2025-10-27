from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StringType

# Inicializar sesión Spark con soporte para Kafka
spark = SparkSession.builder \
    .appName("StreamingSeguridadAlimentaria") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema del mensaje JSON
schema = StructType() \
    .add("municipio", StringType()) \
    .add("genero", StringType()) \
    .add("beneficio", StringType())

# Leer datos en streaming desde Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "seguridad_topic") \
    .load()

# Convertir los valores del mensaje de Kafka a columnas
df_valores = df_stream.selectExpr("CAST(value AS STRING)")
df_json = df_valores.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Procesamiento: contar registros por género y beneficio
df_conteo = df_json.groupBy("genero", "beneficio").count()

# Mostrar resultados en consola en tiempo real
consulta = df_conteo.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

consulta.awaitTermination()
