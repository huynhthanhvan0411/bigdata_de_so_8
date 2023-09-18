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



# ===================3.7=============
# print("Câu 3.7: Những trận của Burnley thi đấu trên sân nhà và có số bàn thắng >= 3 (cả đội khách)")
# cau7 = data.filter((data["HomeTeam"] == "Burnley") & (data["TotalGoals"] >= 3)).count()
# print("Những trận của Burnley thi đấu trên sân nhà và có số bàn thắng >=3 (cả đội khách): " + str(cau7))
# print("==========================================================")

# # # Thêm cột "TotalGoals" để tính tổng số bàn thắng (cả đội nhà và đội khách)
# data_with_total_goals = data.withColumn("TotalGoals", col("FTHG") + col("FTAG"))

# # # Lọc ra các trận của Burnley được thi đấu trên sân nhà và có tổng số bàn thắng >= 3
# burnley_home_matches = data_with_total_goals.filter((col("HomeTeam") == "Burnley") & (col("TotalGoals") >= 3))

# # # Hiển thị thông tin về các trận đó
# burnley_home_matches.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "TotalGoals").show()

# # ===================3.8=============
homeTeam = data.filter((col("HomeTeam") == "Reading") & (col("FTHG") < col("FTAG")))\
                .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG", "HTR")
awayTeam = data.filter((col("AwayTeam") == "Reading") & (col("FTAG") > col("FTHG")))\
                .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG","HTHG", "HTAG", "HTR")

data = homeTeam.union(awayTeam)

# # Hiển thị danh sách các trận mà Reading thua với các cột đã chọn
data.show()

# ===================3.9=============
print("Câu 3.9: Thống kê số lần thắng, hòa, thua của mỗi đội sân nhà")
cau9 = data.groupBy("HomeTeam", "FTR").count().groupBy("HomeTeam").pivot("FTR").agg({"count": "sum"}).na.fill(0)
cau9.show(100)
print("==========================================================")

# ============================

# Câu 3.9: Xoay giá trị trong cột FTR thành các cột, với mỗi cột chứa số lượng FTR tương ứng. Nhóm theo HomeTeam.
cau9_data = data.groupBy("HomeTeam", "FTR").agg(count("FTR").alias("Count"))
pivot_df = cau9_data.groupBy("HomeTeam").pivot("FTR").agg(sum("Count")).na.fill(0)

pivot_df.show(100)
# # ===================3.10=============

# from pyspark.sql.functions import when

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

