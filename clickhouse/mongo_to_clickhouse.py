from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.functions import col
import glob
import os
def load_mongo_to_ch():

    jars_dir = "./jar"
    jar_files = glob.glob(os.path.join(jars_dir, "*.jar"))
    jars_list = ",".join([f"{jar.replace('\\', '/')}" for jar in jar_files])
    print(jars_list)

    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("MongoDB to ClickHouse") \
        .config("spark.local.dir", "D:/spark-temp") \
        .config("spark.jars", jars_list) \
        .getOrCreate()
    #或是用.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.clickhouse:clickhouse-jdbc:0.6.3")

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read \
        .format("mongo") \
        .option("uri", "mongodb://root:example@127.0.0.1:27017/test.users?authSource=admin") \
        .load()

    df.show()
    df.printSchema()
    # 使用col，目標欄位與source相同，選取特定欄位寫入目標，未選取欄位將會是空值
    # processed_df = df.select(
    #     col("name"),
    #     (col("age") + 1)
    #     .alias("age")).na.fill({"age": 0})  # 處理空值 且只會有兩個col放入目標

    # 編輯source特定欄位，目標只包含選擇的欄位
    # processed_df = df.select(df.name, (df.age + 10).alias("age")).na.fill({"age": 0})

    # 編輯source特定欄位，目標需包含source所有欄位，載入時也會將其餘欄位載入
    processed_df = df.withColumn("_id", col("_id").cast("string")) \
       .withColumn("age", col("age") + 10)    

 


    # ClickHouse JDBC 連接參數
    clickhouse_url = "jdbc:clickhouse://127.0.0.1:28123/default"
    driver = "com.clickhouse.jdbc.ClickHouseDriver"
    user = "root" 
    password = "password123"  

    processed_df.write \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "spark_table") \
        .option("driver", driver ) \
        .option("user", user ) \
        .option("password",password ) \
        .mode("append") \
        .save()
    #   target_table，必須已存在
    #   mode可設為override

    spark.stop()

    # Clickhosue-native-connect


load_mongo_to_ch()