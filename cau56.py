from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("FindMaxProduct").getOrCreate()

# Load the data from CSV file
data = spark.read.format('csv')\
    .option('header', 'true')\
    .option('escape', '\"')\
    .load('D:/WORK_UTT_F/nam_4/ki_1/part_1/big_data_demo/nhom6/E1.csv')

# Convert numeric columns to IntegerType
data = data.withColumn("FTHG", data["FTHG"].cast(IntegerType()))
data = data.withColumn("FTAG", data["FTAG"].cast(IntegerType()))
data = data.withColumn("HS", data["HS"].cast(IntegerType()))
data = data.withColumn("AS", data["AS"].cast(IntegerType()))


from pyspark.sql.functions import sum

def cau5():
    print("Câu 3.5: Tìm tổng số bàn thắng các đội ghi được trên sân nhà")
    
    # Group by "HomeTeam" and calculate the sum of "FTHG" as "Total"
    data = df.groupBy("HomeTeam").agg(sum("FTHG").alias("Total"))
    data.show(100)
    
    # Calculate the total sum of "Total" and display it
    total_goals = data.agg(sum("Total").alias("sum")).first().sum
    print("Tổng số bàn thắng các đội đá sân nhà ghi được:", total_goals)
    print("==========================================================")

# Call the cau5 function to execute it
cau5()
