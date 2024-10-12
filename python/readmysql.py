#pip install python-dotenv
#pyspark --conf "spark.driver.extraClassPath=/home/jacob/Development/spark/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar"
#spark-submit --conf "spark.driver.extraClassPath=/home/jacob/Development/spark/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar" readmysql.py 

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASS")
db_key = os.getenv("DB_KEY")

url = "jdbc:mysql://localhost/testdb1"
query = "select id, testcol1, convert(aes_decrypt(testbin, '" + db_key + "') using utf8) as testbin from testtable1"

spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/testdb1") \
    .option("query", query) \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()

df.write.csv("readmysql.out", header=True, mode="overwrite")
