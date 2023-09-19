
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

# 3.4. Tìm số trận có kết quả hoà
# (FTR= Full time result: H = homewin, D = Draw, A= Awaywin)

# # ===================3.4=============
# ta lựa chọn theo cột FTR vì cột FTR là cột full time và kết quả hòa là D=Draw 
# điều kiện ftr ==d là hòa 
cau4 = data.where(data["FTR"] == "D").count()
print("Câu 3.4: Số trận có kết quả hòa")
print("Số trận có kết quả hòa: " + str(cau4))
print("==========================================================")
# # Lọc các trận có kết quả hòa
# th = data.filter(data["FTR"] == "D")

# # Hiển thị thông tin về các đội tham gia trong các trận hòa
# print("Câu 3.4: Các trận đấu có kết quả hòa và các đội tham gia:")
# th.select("HomeTeam", "AwayTeam").show(truncate=False)

