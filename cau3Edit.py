import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct,count,sum,col,when,min,max,avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
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

# ===================3.3=============
print("============================3.3=====================================")
# cau_3 = data.select("HomeTeam").distinct()
# ta lựa chọn data để select theo cột hometeam avf loại bỏ trùng dùng distinct() nhưng vì muốn đổi tên thay vi để 
# homêtam khi in ra ta dùng withcolumnremane đổi hometeam thành list team rồi select và distict là listteam
cau_3 = data.withColumnRenamed("HomeTeam", "List_Team").select("List_Team").distinct()
print("Câu 3.3: Có " + str(cau_3.count()) +" đội trong mùa giải")
print("Danh sách các đội:")
cau_3.show(n=cau_3.count(), truncate=False)

# # ===================3.4=============
# ta lựa chọn theo cột FTR vì cột FTR là cột full time và kết quả hòa là D=Draw 
# điều kiện ftr ==d là hòa 
cau4 = data.where(data["FTR"] == "D")
cau4_count = cau4.count()
print("Câu 3.4: Số trận có kết quả hòa")
print("Số trận có kết quả hòa: " + str(cau4_count))
print("==========================================================")
print("Câu 3.4: Các trận đấu có kết quả hòa và các đội tham gia:")
cau4.select("HomeTeam", "AwayTeam").show(n=cau4.count(), truncate=False)

# 3.5. Tìm tổng số bàn thắng các đội đá trên sân nhà ghi được 
# (FTHG = Fulltime hometeam goal – FTAG: Fulltime AwayGoal)

print("Câu 3.5: Tìm tổng số bàn thắng các đội ghi được trên sân nhà")
cau5 = data.groupBy("HomeTeam").agg({"FTHG": "sum"}).withColumnRenamed("sum(FTHG)", "Total")
cau5.show(n=cau5.count(), truncate=False)
# tính toorng full số bàn thắng ở cột total 
total_goals = cau5.selectExpr("sum(Total) as Total").first().Total
print("Tổng số bàn thắng các đội đá sân nhà ghi được: " + str(total_goals))
print("==========================================================")

# 3.6. Tìm những trận có tổng số bàn thắng > 3
print("Câu 3.6:")
cau6 = data.withColumn("TotalGoals", col("FTHG") + col("FTAG")).filter(col("TotalGoals") > 3)
# tính tổng 
print("Số trận có tổng số bàn thắng > 3: " + str(cau6.count()))
# in hàng
print("Câu 3.6: Những trận có tổng số bàn thắng > 3")
cau6.select("HomeTeam", "AwayTeam", "TotalGoals").show(n=cau6.count(), truncate=False)

# 3.7. Tìm những trận của Burnley được thi đấu trên sân nhà và có số bàn thắng >=3 (Tính cả của đội khách)
# Thêm cột "TotalGoals" để tính tổng số bàn thắng (cả đội nhà và đội khách)
sum_goals = data.withColumn("TotalGoals", col("FTHG") + col("FTAG"))
# Lọc ra các trận của Burnley được thi đấu trên sân nhà và có tổng số bàn thắng >= 3
cau7 = sum_goals.filter((col("HomeTeam") == "Burnley") & (col("TotalGoals") >= 3))
# Hiển thị thông tin về các trận đó
print("Câu 3.7: Những trận của Burnley thi đấu trên sân nhà và có số bàn thắng >= 3 (cả đội khách):" + str(cau7.count()))
cau7.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "TotalGoals").show(n=cau7.count(), truncate=False)

# 3.8. Tìm những trận mà Reading thua (Không được sử dụng cột FTR)
homeTeam = data.filter((col("HomeTeam") == "Reading") & (col("FTHG") < col("FTAG")))\
                .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG", "HTR")
awayTeam = data.filter((col("AwayTeam") == "Reading") & (col("FTAG") > col("FTHG")))\
                .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG","HTHG", "HTAG", "HTR")
cau8 = homeTeam.union(awayTeam)
# # Hiển thị danh sách các trận mà Reading thua với các cột đã chọn
cau8.show(n=cau8.count(), truncate=False)

# 3.9. Xoay giá trị trong cột FTR thành các cột, với mỗi cột chứa số lượng FTR tương ứng. nhóm theo HomeTeam
cau9_data = data.groupBy("HomeTeam", "FTR").agg(count("FTR").alias("Count"))
cau9 = cau9_data.groupBy("HomeTeam").pivot("FTR").agg(sum("Count")).na.fill(0)
# print("so cot"+str(cau9.count()))
cau9.show(n=cau9.count(), truncate=False)

# 3.10. Tạo một cột mới với tên cột tuỳ chọn: Nếu tổng số bàn thắng 2 đội ghi được trong trận  <2 thì điền “well” , nếu số bàn thắng  2 < x < 4 thì điền “very good”, nếu số bàn thắng >= 4 thì điền “amazing”. 
# Tính tổng số bàn thắng trong trận đấu và thêm cột "Status" dựa trên tổng số bàn thắng
cau10 = data.withColumn(
    "GoalsTotal",
    data["FTHG"] + data["FTAG"]
)
# Tính toán cột "Status" dựa trên "GoalsTotal"
cau10 = cau10.withColumn(
    "Status",
    when(cau10["GoalsTotal"] < 2, "well")
    .when((cau10["GoalsTotal"] >= 2) & (cau10["GoalsTotal"] < 4), "very good")
    .otherwise("amazing")
)
# Hiển thị kết quả
cau10.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "GoalsTotal", "Status").show(n=cau10.count(), truncate=False)