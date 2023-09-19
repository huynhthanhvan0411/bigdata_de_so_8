
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

# 3.8. Tìm những trận mà Reading thua (Không được sử dụng cột FTR)

homeTeam = data.filter((col("HomeTeam") == "Reading") & (col("FTHG") < col("FTAG")))\
                .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG", "HTR")
awayTeam = data.filter((col("AwayTeam") == "Reading") & (col("FTAG") > col("FTHG")))\
                .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG","HTHG", "HTAG", "HTR")

data = homeTeam.union(awayTeam)

# # Hiển thị danh sách các trận mà Reading thua với các cột đã chọn
data.show()