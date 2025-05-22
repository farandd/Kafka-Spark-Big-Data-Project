from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("ConsumerSensorGudang") \
    .getOrCreate()

# Schema sensor suhu
schema_suhu = StructType([
    StructField("gudang_id", StringType()),
    StructField("suhu", IntegerType())
])

# Schema sensor kelembaban
schema_kelembaban = StructType([
    StructField("gudang_id", StringType()),
    StructField("kelembaban", IntegerType())
])

# Baca stream sensor suhu
stream_suhu = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*") \
    .withWatermark("suhu", "10 seconds")  # Watermark tidak wajib di sini, tapi baik untuk window join

# Baca stream sensor kelembaban
stream_kelembaban = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembaban).alias("data")) \
    .select("data.*") \
    .withWatermark("kelembaban", "10 seconds")

# Filter suhu tinggi
suhu_tinggi = stream_suhu.filter(col("suhu") > 80) \
    .selectExpr("gudang_id", "suhu", "'[Peringatan Suhu Tinggi]' as peringatan")

# Filter kelembaban tinggi
kelembaban_tinggi = stream_kelembaban.filter(col("kelembaban") > 70) \
    .selectExpr("gudang_id", "kelembaban", "'[Peringatan Kelembaban Tinggi]' as peringatan")

# Join stream berdasarkan gudang_id dan waktu window 10 detik
# Karena data tidak ada timestamp eksplisit, kita bisa menggunakan processing time join (approximate join)
joined = suhu_tinggi.join(kelembaban_tinggi, "gudang_id") \
    .filter((col("suhu") > 80) & (col("kelembaban") > 70)) \
    .select(
        col("gudang_id"),
        col("suhu"),
        col("kelembaban")
    )

def foreach_batch_function(df, epoch_id):
    if df.count() == 0:
        return
    # Collect data untuk output manual
    data = df.collect()
    for row in data:
        print(f"[PERINGATAN KRITIS] Gudang {row['gudang_id']}:")
        print(f" - Suhu: {row['suhu']}°C")
        print(f" - Kelembaban: {row['kelembaban']}%")
        print(" - Status: Bahaya tinggi! Barang berisiko rusak\n")

query = joined.writeStream.foreachBatch(foreach_batch_function).start()

# Output warning suhu dan kelembaban secara terpisah
def warn_suhu(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        print(f"[Peringatan Suhu Tinggi] Gudang {r['gudang_id']}: Suhu {r['suhu']}°C")

def warn_kelembaban(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        print(f"[Peringatan Kelembaban Tinggi] Gudang {r['gudang_id']}: Kelembaban {r['kelembaban']}%")

query_suhu = suhu_tinggi.writeStream.foreachBatch(warn_suhu).start()
query_kelembaban = kelembaban_tinggi.writeStream.foreachBatch(warn_kelembaban).start()

# Tunggu sampai stream selesai (ctrl+c untuk stop)
query.awaitTermination()
query_suhu.awaitTermination()
query_kelembaban.awaitTermination()
