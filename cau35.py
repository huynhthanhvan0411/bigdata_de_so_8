
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

# 3.5. Tìm tổng số bàn thắng các đội đá trên sân nhà ghi được 
# (FTHG = Fulltime hometeam goal – FTAG: Fulltime AwayGoal)

print("Câu 3.5: Tìm tổng số bàn thắng các đội ghi được trên sân nhà")
cau5 = data.groupBy("HomeTeam").agg({"FTHG": "sum"}).withColumnRenamed("sum(FTHG)", "Total")
cau5.show(100)
total_goals = cau5.selectExpr("sum(Total) as Total").first().Total
print("Tổng số bàn thắng các đội đá sân nhà ghi được: " + str(total_goals))
print("==========================================================")