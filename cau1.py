# Nhập các thư viện cần thiết
from pyspark.sql import SparkSession
# Tạo một phiên Spark
spark = SparkSession.builder.appName("Championship2011").getOrCreate()
# Đọc dữ liệu từ tệp CSV và gán cho một DataFrame
df=spark.read.csv("đường_dẫn_đến_tệp/E1.csv",header=True,inferSchema=True)
# Hiển thị dữ liệu trong DataFrame
df.show()
