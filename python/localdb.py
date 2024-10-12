spark = spark.builder.appName('test').master('local[*]').config("spark.driver.extraClassPath", "/home/jacob/Development/spark/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar").getOrCreate()

df = spark.read.format("jdbc").option("url","jdbc:mysql://localhost/testdb1").option("driver","com.mysql.jdbc.Driver").option("dbtable","testtable1").option("user","spark").option("password","password").load()
