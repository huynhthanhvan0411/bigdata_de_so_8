import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("FindMaxProduct").getOrCreate()

# Define the schema with specified data types
schema = StructType([
    StructField("Div", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("HomeTeam", StringType(), True),
    StructField("AwayTeam", StringType(), True),
    StructField("FTHG", IntegerType(), True),
    StructField("FTAG", IntegerType(), True),
    StructField("FTR", StringType(), True),
    StructField("HTHG", IntegerType(), True),
    StructField("HTAG", IntegerType(), True),
    StructField("HTR", StringType(), True),
    StructField("HS", IntegerType(), True),
    StructField("AS", IntegerType(), True),
    StructField("HST", StringType(), True),
    StructField("AST", StringType(), True),
    StructField("HF", StringType(), True),
    StructField("AF", StringType(), True),
    StructField("HC", StringType(), True),
    StructField("AC", StringType(), True),
    StructField("HY", StringType(), True),
    StructField("AY", StringType(), True),
    StructField("HR", StringType(), True),
    StructField("AR", StringType(), True),
    StructField("B365H", StringType(), True),
    StructField("B365D", StringType(), True),
    StructField("B365A", StringType(), True),
])

# Load the CSV data with the defined schema
data = spark.read.format('csv') \
    .option('header', 'true') \
    .option('escape', '\"') \
    .schema(schema) \
    .load('D:/WORK_UTT_F/nam_4/ki_1/part_1/big_data_demo/nhom6/E1.csv')
# Function to calculate and display the results for each query
def cau3_schema():
 data.printSchema()

def cau_3():
    # ===================3.3=============
    # cau_3 = data.select("HomeTeam").distinct()
    # ta lựa chọn data để select theo cột hometeam avf loại bỏ trùng dùng distinct() nhưng vì muốn đổi tên thay vi để 
    # homêtam khi in ra ta dùng withcolumnremane đổi hometeam thành list team rồi select và distict là listteam
    cau_3 = data.withColumnRenamed("HomeTeam", "List_Team").select("List_Team").distinct()
    print("Câu 3.3: Có " + str(cau_3.count()) +" đội trong mùa giải")
    print("Danh sách các đội:")
    cau_3.show(n=cau_3.count(), truncate=False)

def cau_4():
    # ta lựa chọn theo cột FTR vì cột FTR là cột full time và kết quả hòa là D=Draw 
    # điều kiện ftr ==d là hòa 
    cau4 = data.where(data["FTR"] == "D")
    cau4_count = cau4.count()
    print("Câu 3.4: Số trận có kết quả hòa")
    print("Số trận có kết quả hòa: " + str(cau4_count))
    print("==========================================================")
    print("Câu 3.4: Các trận đấu có kết quả hòa và các đội tham gia:")
    cau4.select("HomeTeam", "AwayTeam").show(n=cau4.count(), truncate=False)

def cau_5():
    # 3.5. Tìm tổng số bàn thắng các đội đá trên sân nhà ghi được 
    # (FTHG = Fulltime hometeam goal – FTAG: Fulltime AwayGoal)

    print("Câu 3.5: Tìm tổng số bàn thắng các đội ghi được trên sân nhà")
    cau5 = data.groupBy("HomeTeam").agg({"FTHG": "sum"}).withColumnRenamed("sum(FTHG)", "Total")
    cau5.show(n=cau5.count(), truncate=False)
    # tính toorng full số bàn thắng ở cột total 
    total_goals = cau5.selectExpr("sum(Total) as Total").first().Total
    print("Tổng số bàn thắng các đội đá sân nhà ghi được: " + str(total_goals))
def cau_6():
    # 3.6. Tìm những trận có tổng số bàn thắng > 3
    print("Câu 3.6:")
    cau6 = data.withColumn("TotalGoals", col("FTHG") + col("FTAG")).filter(col("TotalGoals") > 3)
    # tính tổng 
    print("Số trận có tổng số bàn thắng > 3: " + str(cau6.count()))
    # in hàng
    print("Câu 3.6: Những trận có tổng số bàn thắng > 3")
    cau6.select("HomeTeam", "AwayTeam", "TotalGoals").show(n=cau6.count(), truncate=False)

def cau_7():
        # 3.7. Tìm những trận của Burnley được thi đấu trên sân nhà và có số bàn thắng >=3 (Tính cả của đội khách)
    # Thêm cột "TotalGoals" để tính tổng số bàn thắng (cả đội nhà và đội khách)
    sum_goals = data.withColumn("TotalGoals", col("FTHG") + col("FTAG"))
    # Lọc ra các trận của Burnley được thi đấu trên sân nhà và có tổng số bàn thắng >= 3
    cau7 = sum_goals.filter((col("HomeTeam") == "Burnley") & (col("TotalGoals") >= 3))
    # Hiển thị thông tin về các trận đó
    print("Câu 3.7: Những trận của Burnley thi đấu trên sân nhà và có số bàn thắng >= 3 (cả đội khách):" + str(cau7.count()))
    cau7.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "TotalGoals").show(n=cau7.count(), truncate=False)

def cau_8():
        # 3.8. Tìm những trận mà Reading thua (Không được sử dụng cột FTR)
    homeTeam = data.filter((col("HomeTeam") == "Reading") & (col("FTHG") < col("FTAG")))\
                    .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG", "HTR")
    awayTeam = data.filter((col("AwayTeam") == "Reading") & (col("FTAG") > col("FTHG")))\
                    .select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG","HTHG", "HTAG", "HTR")
    cau8 = homeTeam.union(awayTeam)
    # # Hiển thị danh sách các trận mà Reading thua với các cột đã chọn
    print("Câu 3.8: Những trận mà đội Reading thua")
    cau8.show(n=cau8.count(), truncate=False)
def cau_9():
        # 3.9. Xoay giá trị trong cột FTR thành các cột, với mỗi cột chứa số lượng FTR tương ứng. nhóm theo HomeTeam
    cau9_data = data.groupBy("HomeTeam", "FTR").agg(count("FTR").alias("Count"))
    cau9 = cau9_data.groupBy("HomeTeam").pivot("FTR").agg(sum("Count")).na.fill(0)
    # print("so cot"+str(cau9.count()))
    print("Câu 3.9: ")
    cau9.show(n=cau9.count(), truncate=False)
def cau_10():
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
    print("Câu 3.10")
    cau10.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "GoalsTotal", "Status").show(n=cau10.count(), truncate=False)

def cau_4_stream():
    # Đọc dữ liệu sử dụng readStream
    df = spark.readStream \
        .format("csv") \
        .option("header", "true").schema(schema) \
        .load("stream/")

    # dfs = df.select("Date", "Time","HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG")
    dfs = df.select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "HTHG", "HTAG")
    goal_counts = dfs.filter((col("FTHG") == col("HTHG")) & (col("FTAG") == col("HTAG")))

    result = goal_counts.withColumn("TotalGoals", col("FTHG") + col("FTAG"))

    # Ghi ra số bàn thắng
    query = result.writeStream \
        .outputMode("append") \
        .format("console")\
        .option('truncate', 'false') \
        .option('numRows', 1000)\
        .start()


    # Chờ cho đến khi người dùng dừng ứng dụng
    query.awaitTermination()

def run_full_code():
    cau3_schema()
    cau_3()
    cau_4()
    cau_5()
    cau_6()
    cau_7()
    cau_8()
    cau_9()
    cau_10()


def exit_program():
    print("Exiting the program...")
    exit()

# Define a dictionary that maps user choices to functions
menu_options = {
    1:cau_3,
    2:cau_4,
    3:cau_5,
    4:cau_6,
    5:cau_7,
    6:cau_8,
    7:cau_9,
    8:cau_10,
    9:run_full_code,
    10: cau_4_stream,
    11: cau3_schema,
    0: exit_program
}

# Main menu loop
while True:
    print("========== MENU ==========")
    print("1. Câu 3.3")
    print("2. Câu 3.4")
    print("3. Câu 3.5")
    print("4. Câu 3.6")
    print("5. Câu 3.7")
    print("6. Câu 3.8")
    print("7. Câu 3.9")
    print("8. Câu 3.10")
    print("9. Chạy từ Câu 3.3 đến Câu 3.10")
    print("10. Chạy câu 4")
    # print("11. Chạy câu 3.2")
    print("0. Exit")
    try:
        choice = int(input("Enter your choice: "))
        selected_function = menu_options.get(choice)
        if selected_function:
            selected_function()
        else:
            print("Invalid choice. Please select a valid option (1-5) or 0 to exit.")
    except ValueError:
        print("Invalid input. Please enter a number.")


spark.stop()
