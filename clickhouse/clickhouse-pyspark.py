from pyspark.sql import SparkSession
def etl_script():
    # 設定 ClickHouse JDBC 連接 URL 和驅動程序位置
    
    clickhouse_driver = "./jar/clickhouse-jdbc-0.6.3.jar"  # 這裡填入你下載的 clickhouse-jdbc.jar 路徑

    # 初始化 SparkSession
    spark = SparkSession.builder \
        .appName("PySpark ClickHouse Example") \
        .config("spark.jars", clickhouse_driver) \
        .getOrCreate()


    driver = "com.clickhouse.jdbc.ClickHouseDriver"
    # 連接 ClickHouse 並加載數據
    url = "jdbc:ch://localhost:8123/default"
    user = "root" 
    password = "password123"  
    query = "select * from users"
    driver = "com.clickhouse.jdbc.ClickHouseDriver"

    df = (spark.read
        .format('jdbc')
        .option('driver', driver)
        .option('url', url)
        .option('user', user)
        .option('password', password)
        .option('query', query).load())

    df.show()

    # 停止 SparkSession
    # spark.stop()


etl_script()