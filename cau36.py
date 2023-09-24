
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import count, sum
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import col, min, max, avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType, StructField, StringType
import logging


spark = SparkSession.builder.appName("FindMaxProduct").getOrCreate()
# ===================3.1=============
data = spark.read.format('csv')\
            .option('header', 'true')\
            .option('escape', '\"')\
            .load('D:/WORK_UTT_F/nam_4/ki_1/part_1/big_data_demo/nhom6/E1.csv')
# ===================3.2=============
data = data.withColumn("FTHG", data["FTHG"].cast(IntegerType()))
data = data.withColumn("FTAG", data["FTAG"].cast(IntegerType()))
data = data.withColumn("HS", data["HS"].cast(IntegerType()))
data = data.withColumn("AS", data["AS"].cast(IntegerType()))

# 3.6. Tìm những trận có tổng số bàn thắng > 3
print("Câu 3.6:")
cau6 = data.withColumn("TotalGoals", col("FTHG") + col("FTAG")).filter(col("TotalGoals") > 3)
# tính tổng 
print("Số trận có tổng số bàn thắng > 3: " + str(cau6.count()))
# in hàng
print("Câu 3.6: Những trận có tổng số bàn thắng > 3")
cau6.select("HomeTeam", "AwayTeam", "TotalGoals").show(n=cau6.count(), truncate=False)