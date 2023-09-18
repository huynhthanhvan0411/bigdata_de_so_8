import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define schema for DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Div", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("HomeTeam", StringType(), True),
    StructField("AwayTeam", StringType(), True),
    StructField("FTHG", IntegerType(), True),
    StructField("FTAG", IntegerType(), True),
    StructField("FTR", StringType(), True),
    StructField("HTHG", IntegerType(), True),
    StructField("HTAG", IntegerType(), True),
    StructField("HTR", StringType(), True),
    StructField("HS", IntegerType(), True),
    StructField("AS", IntegerType(), True),
    StructField("HST", StringType(), True),
    StructField("AST", StringType(), True),
    StructField("HF", StringType(), True),
    StructField("AF", StringType(), True),
    StructField("HC", StringType(), True),
    StructField("AC", StringType(), True),
    StructField("HY", StringType(), True),
    StructField("AY", StringType(), True),
    StructField("HR", StringType(), True),
    StructField("AR", StringType(), True),
    StructField("B365H", StringType(), True),
    StructField("B365D", StringType(), True),
    StructField("B365A", StringType(), True),
])

# Đọc dữ liệu từ file CSV: Trước tiên, bạn cần đọc dữ liệu từ tệp CSV của giải đấu La Liga từ năm 2018 đến 2021.

# Xác định các trận đấu được kết thúc trong một hiệp: Bạn cần kiểm tra xem trận đấu nào không có bàn thắng ở hiệp thứ 2. Điều này có nghĩa là (HTHG + FTAG) == FTHG và (HTAG + HG) == FTAG.

# Ghi ra số bàn thắng ghi được trong trận đấu đó: Sau khi xác định các trận đấu phù hợp, bạn có thể tính tổng số bàn thắng ghi được trong trận đấu bằng cách thêm FTHG và FTAG lại với nhau.

# Create a SparkSession with userClassPathFirst set to true
spark = SparkSession.builder \
    .appName("FindMaxProduct") \
    .config("spark.driver.userClassPathFirst", "true") \
    .getOrCreate()

# Read the CSV file
data = spark.readStream \
    .schema(schema) \
    .option('header', 'true') \
    .format("csv") \
    .option('path', 'D:/WORK_UTT_F/nam_4/ki_1/part_1/big_data_demo/nhom6/stream/') \
    .load()


# Perform your data processing and filtering here
data_filtered = data.where((col("HTHG") == col("FTHG")) & (col("HTAG") == col("FTAG"))) \
    .withColumn("Total", col("FTHG") + col("FTAG"))

# Define your streaming query
query = data_filtered.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(lambda batch, batchId: batch.show(100)) \
    .start()

# Await the termination of the streaming query
query.awaitTermination()
