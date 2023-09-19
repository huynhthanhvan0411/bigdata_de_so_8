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


# ===================3.3=============
print("============================3.3=====================================")
# cau_3 = data.select("HomeTeam").distinct()
# ta lựa chọn data để select theo cột hometeam avf loại bỏ trùng dùng distinct() nhưng vì muốn đổi tên thay vi để 
# homêtam khi in ra ta dùng withcolumnremane đổi hometeam thành list team rồi select và distict là listteam
cau_3 = data.withColumnRenamed("HomeTeam", "List_Team").select("List_Team").distinct()
print("Câu 3.3: Có " + str(cau_3.count()) +" đội trong mùa giải")
print("Danh sách các đội:")
cau_3.show(n=cau_3.count(), truncate=False)

