import findspark
import mysql.connector

from configFileReader import user, password, result_file_path, host, sqlQueryAfterTransformation
from pyspark.sql.functions import col, collect_set, dense_rank, rank, percent_rank
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

findspark.init("C:\spark\spark-3.3.1-bin-hadoop3")

# Prepare MySql connection.
connection = mysql.connector.connect(
    user=user,
    password=password,
    host=host
)
# create mycursor object to make the connection for executing SQL queries
mycursor = connection.cursor()

spark = SparkSession.builder \
    .master("local") \
    .appName("test") \
    .config("spark.driver.extraClassPath", "C:\spark\mysql-connector-java-8.0.30.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
empDF = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/org?allowPublicKeyRetrieval=true&useSSL=false") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "employee") \
    .option("user", user) \
    .option("password", password) \
    .load()

deptDF = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/org?allowPublicKeyRetrieval=true&useSSL=false") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "department") \
    .option("user", user) \
    .option("password", password) \
    .load()

# Adding new column
addColDf = empDF.withColumn("row_created_timestamp", current_timestamp())

# Aggregate Function
AggResult = empDF.select(collect_set("salary"))

# Window Function
windowFun = Window.partitionBy("employee_id").orderBy("salary")

# row_number Window Function
rowNumDf = addColDf.withColumn("rownumber",row_number().over(windowFun))

#rank Window Function
#Sort by ascending order of salary and have skip rankings for manager_id.
rankDf = rowNumDf.withColumn('salary_rank',rank().over(windowFun.orderBy(col('salary').asc())))

# dense_rank Window Function
#Sort by descending order of salary and have continuous ranking for manager_id.
denseRankFun = rankDf.withColumn('salary_dense_rank',dense_rank().over(windowFun.orderBy(col('salary').desc())))

# percent_rank Window Function
#Sort and generating a relative/percent rank to distance from max salary.
perRankFun = denseRankFun.withColumn('salary_per_rank',percent_rank().over(windowFun.orderBy(col('salary').desc())))\
    .select("employee_id","first_name","salary","department_id","row_created_timestamp","rownumber","salary_rank","salary_dense_rank","salary_per_rank")

# Joining perRankFun and department dataframe
joinResultDf = perRankFun.join(deptDF, perRankFun.department_id == deptDF.department_id, "inner") \
    .select("employee_id","first_name","salary",perRankFun["department_id"],"row_created_timestamp",
            "rownumber","salary_rank","salary_dense_rank","salary_per_rank",
            "department_name",deptDF["manager_id"])
joinResultDf.write.option("header", True) \
    .mode("overwrite").csv(result_file_path)

with open(sqlQueryAfterTransformation, 'r') as sql_file:
    result_iterator = mycursor.execute(sql_file.read(), multi=True)
    for res in result_iterator:
        # Will print out a short representation of the query
        print("Running query: ", res)
        print(f"Affected {res.rowcount} rows")

    connection.commit()
joinResultDf.write.mode("Append").format("jdbc") \
   .option("url", "jdbc:mysql://localhost:3306/org?allowPublicKeyRetrieval=true&useSSL=false") \
   .option("driver", "com.mysql.cj.jdbc.Driver") \
   .option("dbtable", "transformation_result") \
   .option("user", user) \
   .option("password", password) \
   .save()
