
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

# 3.9. Xoay giá trị trong cột FTR thành các cột, với mỗi cột chứa số lượng FTR tương ứng. nhóm theo HomeTeam
cau9_data = data.groupBy("HomeTeam", "FTR").agg(count("FTR").alias("Count"))
cau9 = cau9_data.groupBy("HomeTeam").pivot("FTR").agg(sum("Count")).na.fill(0)
# print("so cot"+str(cau9.count()))
cau9.show(n=cau9.count(), truncate=False)

