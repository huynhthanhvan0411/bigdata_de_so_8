
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

# 3.10. Tạo một cột mới với tên cột tuỳ chọn: Nếu tổng số bàn thắng 2 đội ghi được trong trận  <2 thì điền “well” , nếu số bàn thắng  2 < x < 4 thì điền “very good”, nếu số bàn thắng >= 4 thì điền “amazing”. 

print("Câu 3.10: Tạo cột Status dựa trên tổng số bàn thắng")
cau10 = data.withColumn("TotalGoals", data["FTHG"] + data["FTAG"]).withColumn(
    "Status",
    when(data["TotalGoals"] < 2, "well")
    .when((data["TotalGoals"] >= 2) & (data["TotalGoals"] < 4), "very good")
    .otherwise("amazing")
)
cau10.select("DIV", "DATE", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "TotalGoals", "Status").show(100)
print("==========================================================")
# Tạo cột "TotalGoals" bằng cách thêm số bàn thắng của đội nhà (FTHG) và đội khách (FTAG)
data = data.withColumn("TotalGoals", data["FTHG"] + data["FTAG"])

# # Tính toán cột "Status" dựa trên "TotalGoals"
data = data.withColumn(
    "Status",
    when(data["TotalGoals"] < 2, "well")
    .when((data["TotalGoals"] >= 2) & (data["TotalGoals"] < 4), "very good")
    .otherwise("amazing")
)

# # Hiển thị kết quả
data.select("DIV", "DATE", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "TotalGoals", "Status").show(100)

