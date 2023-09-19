
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

# 3.7. Tìm những trận của Burnley được thi đấu trên sân nhà và có số bàn thắng >=3 (Tính cả của đội khách)

from pyspark.sql.functions import col

# Thêm cột "TotalGoals" để tính tổng số bàn thắng (cả đội nhà và đội khách)
data_with_total_goals = data.withColumn("TotalGoals", col("FTHG") + col("FTAG"))

# Lọc ra các trận của Burnley được thi đấu trên sân nhà và có tổng số bàn thắng >= 3
burnley_home_matches = data_with_total_goals.filter((col("HomeTeam") == "Burnley") & (col("TotalGoals") >= 3))

# Hiển thị thông tin về các trận đó
print("Câu 3.7: Những trận của Burnley thi đấu trên sân nhà và có số bàn thắng >= 3 (cả đội khách):")
burnley_home_matches.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "TotalGoals").show()

