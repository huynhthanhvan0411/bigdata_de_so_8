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

# Create a Spark session
spark = SparkSession.builder.appName("FindMaxProduct").getOrCreate()

# Load the CSV data
data = spark.read.format('csv')\
            .option('header', 'true')\
            .option('escape', '\"')\
            .load('D:/WORK_UTT_F/nam_4/ki_1/part_1/big_data_demo/nhom6/E1.csv')

# Function to calculate and display the results for each query
def run_query(query_num):
    if query_num == 1:
        cau_3 = data.withColumnRenamed("HomeTeam", "List_Team").select("List_Team").distinct()
        print("Câu 3.3: Có " + str(cau_3.count()) + " đội trong mùa giải")
        print("Danh sách các đội:")
        cau_3.show(n=cau_3.count(), truncate=False)
    elif query_num == 2:
        cau4 = data.where(data["FTR"] == "D")
        cau4_count = cau4.count()
        print("Câu 3.4: Số trận có kết quả hòa")
        print("Số trận có kết quả hòa: " + str(cau4_count))
        print("==========================================================")
        print("Câu 3.4: Các trận đấu có kết quả hòa và các đội tham gia:")
        cau4.select("HomeTeam", "AwayTeam").show(n=cau4.count(), truncate=False)
    elif query_num == 3:
        cau5 = data.groupBy("HomeTeam").agg({"FTHG": "sum"}).withColumnRenamed("sum(FTHG)", "Total")
        cau5.show(n=cau5.count(), truncate=False)
        total_goals = cau5.selectExpr("sum(Total) as Total").first().Total
        print("Tổng số bàn thắng các đội đá sân nhà ghi được: " + str(total_goals))
        print("==========================================================")
    elif query_num == 4:
        cau6 = data.withColumn("TotalGoals", col("FTHG") + col("FTAG")).filter(col("TotalGoals") > 3)
        print("Câu 3.6: Số trận có tổng số bàn thắng > 3: " + str(cau6.count()))
        print("Câu 3.6: Những trận có tổng số bàn thắng > 3")
        cau6.select("HomeTeam", "AwayTeam", "TotalGoals").show(n=cau6.count(), truncate=False)
    elif query_num == 0:
        print("Exiting the program...")
        exit()
    else:
        print("Invalid choice. Please select a valid option (1-4) or 0 to exit.")

# Main menu loop
while True:
    print("========== MENU ==========")
    print("1. Câu 3.3")
    print("2. Câu 3.4")
    print("3. Câu 3.5")
    print("4. Câu 3.6")
    print("1. Câu 3.3")
    print("2. Câu 3.4")
    print("3. Câu 3.5")
    print("4. Câu 3.6")
    print("0. Exit")
    print("0. Exit")
    try:
        choice = int(input("Enter your choice: "))
        run_query(choice)
    except ValueError:
        print("Invalid input. Please enter a number.")

# Stop the Spark session when the program exits (this should be done at the end of your script)
spark.stop()
