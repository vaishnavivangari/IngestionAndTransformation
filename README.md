# IngestionAndTransformation

## Problem Statement


-	Write a utility to read data from MySQL/Oracle DB.

-	The utility will establish a connection to the DB and read the data.

-	Apply few transformations, perform few actions, windowing functions, aggregate functions, etc,.

-	Prepare a final dataset.

-	Write a utility to write the final dataset to GCP BigQuery.

-	The utility will establish a connection to BQ (using BQ-Connectors) and write the data to BQ table.


# Tech stack
## Components	Technologies

- Operating systems 	:Windows

- Programming languages	:Java, Python, Pyspark

- Data storage and querying 	 :MySQL, BigQuery

- Backend Frameworks	 :Intellij

## Versions

-	Python 3.9

-	Spark-3.3.1-bin-hadoop3

-	Hadoop 3.2.2

-	Java 1.8

-	BigQuery


# Data Flow Architecture
 
![image](https://user-images.githubusercontent.com/115626549/214585135-c910d623-49dc-4079-b627-ff9abe495a5f.png)


# Import MySql connector and config file. 
    import mysql.connector
    from configFileReader import user,password,host,dbName,sqlQuery

# Prepare MySql connection.
    connection = mysql.connector.connect(
        user=user,
        password=password,
        host=host
    )
# Create mycursor object to make the connection for executing SQL queries
    mycursor = connection.cursor()
    with open(sqlQuery, 'r') as sql_file:
        result_iterator = mycursor.execute(sql_file.read(), multi=True)
        for res in result_iterator:
            # Will print out a short representation of the query
            print("Running query: ", res)
            print(f"Affected {res.rowcount} rows")
        connection.commit()
        
# Applied some transformation,action,window funnctions and etc
    import findspark
    from configFileReader import user, password, result_file_path
    from pyspark import StorageLevel
    from pyspark.sql.functions import col, collect_set, dense_rank, rank, percent_rank
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    findspark.init("C:\spark\spark-3.3.1-bin-hadoop3")
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
    addColDf.select(first("salary"))
    addColDf.select(mean("salary"))

# Window Function
    windowFun = Window.partitionBy("employee_id").orderBy("salary")

# row_number Window Function
    rowNumDf = addColDf.withColumn("row_number",row_number().over(windowFun))

# rank Window Function
### Sort by ascending order of salary and have skip rankings for employee_id.
    rankDf = rowNumDf.withColumn('salary_rank',rank().over(windowFun.orderBy(col('salary').asc())))

# dense_rank Window Function
### Sort by descending order of salary and have continuous ranking for manager_id.
    denseRankFun = rankDf.withColumn('salary_dense_rank',dense_rank().over(windowFun.orderBy(col('salary').desc())))

# percent_rank Window Function
### Sort and generating a relative/percent rank to distance from max salary.
    perRankFun = denseRankFun.withColumn('salary_per_rank',percent_rank().over(windowFun.orderBy(col('salary').desc())))\
        .select("employee_id","first_name","salary","department_id","row_created_timestamp","row_number","salary_rank","salary_dense_rank","salary_per_rank")

# Joining perRankFun and department dataframe
    joinResultDf = perRankFun.join(deptDF, perRankFun.department_id == deptDF.department_id, "inner") \
        .select("employee_id","first_name","salary",perRankFun["department_id"],"row_created_timestamp",
                "row_number","salary_rank","salary_dense_rank","salary_per_rank",
                "department_name",deptDF["manager_id"])
    joinResultDf.write.option("header", True) \
        .mode("overwrite").csv(result_file_path)

# Prepare BigQuery connection.  
    import os
    from google.oauth2 import service_account
    from google.cloud import bigquery
    from pathlib import Path
    from configFileReader import credentials,project_id,dataset_id,table_id,result_file_path
    
    credentials = service_account.Credentials.from_service_account_file(
        credentials)

# Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=project_id)


# CREATE dataset
    def create_dataset(datasetId):
        dataset_ref = bigquery.DatasetReference.from_string(datasetId, default_project=client.project)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        try:
            dataset = client.create_dataset(dataset)
            print("Dataset is created {}.{} successfully ".format(client.project, dataset.dataset_id))
        except:
            print("Dataset is already exists.")

# Load local file into bigquery table
    def load_table_uri_autodetect_csv(dataFileFolder, tableId: str):
        global csvFile
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="row_created_timestamp",
            )
        )
        for file in os.listdir(dataFileFolder):
            if file.endswith('.csv'):
                print('Processing file: {0}'.format(file))
                csvFile = dataFileFolder.joinpath(file)
        with open(csvFile, 'rb') as source_file:
            job = client.load_table_from_file(source_file, tableId, job_config=job_config)
        job.result()
        table = client.get_table(tableId)
        print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), tableId))
        
    create_dataset(datasetId=dataset_id)
    load_table_uri_autodetect_csv(Path(result_file_path), table_id)
