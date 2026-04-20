from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("TicketAnalysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("ticket_id", IntegerType()) \
    .add("client_id", IntegerType()) \
    .add("timestamp", StringType()) \
    .add("demande", StringType()) \
    .add("type_demande", StringType()) \
    .add("priorite", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "client_tickets") \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Exemple d'analyse : nombre de tickets par priorité
df_analysis = df_parsed.groupBy("priorite").count()

query = df_analysis.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .start()

query.awaitTermination()