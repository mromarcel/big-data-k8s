# import libraries
import pytz
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import DateType, StringType, TimestampType, DecimalType, LongType, StructField, StructType
from pyspark.sql.functions import current_timestamp, col

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("etl-enriched-users-analysis") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.deepstorage.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "myaccesskey") \
        .config("spark.hadoop.fs.s3a.secret.key", "mysecretkey") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.fast.upload", True) \
        .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
        .config("fs.s3a.connection.maximum", 100) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .enableHiveSupport() \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.db", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.db.type", "hive") \
        .config("spark.sql.catalog.db.uri", "thrift://hive.metastore.svc.cluster.local:9083") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.catalog.db.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.db.warehouse", "s3a://warehouse/metastore") \
        .config("spark.sql.catalog.db.s3.endpoint", "http://minio.deepstorage.svc.cluster.local:9000") \
        .config("spark.sql.defaultCatalog", "db") \
        .getOrCreate()
        #.config("spark.sql.catalog.db", "org.apache.iceberg.spark.SparkCatalog") \
        #.config("spark.sql.catalog.db.type", "hadoop") \
        #.config("spark.sql.catalog.db.s3.endpoint", "http://minio.deepstorage.svc.cluster.local:9000") \
        #.config("spark.sql.catalog.db.warehouse", "s3a://hive/warehouse/") \
        #.getOrCreate()
        #

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    # ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    spark.sparkContext.setLogLevel("FATAL")
    
    # Get the past 1 hours interval 
    # PAST_HOUR_PREFIX = (datetime.now(pytz.timezone('America/Sao_Paulo'))-timedelta(hours=1) ).strftime("%Y/%m/%d/%H/")
    CURRENT_HOUR_PREFIX = (datetime.now(pytz.timezone('America/Sao_Paulo')) ).strftime("%Y/%m/%d/%H/")

    get_subscription_file =  "s3a://datalake/raw/src-app-brazilian-subscription-json/" + CURRENT_HOUR_PREFIX

    customSchema = StructType([
        StructField("address",StringType(),True),
        StructField("birth_date",StringType(),True),
        StructField("createdAt",StringType(),True),
        StructField("gender",StringType(),True),
        StructField("id",LongType(),True),
        StructField("payment_method",StringType(),True),
        StructField("payment_term",StringType(),True),
        StructField("plan",StringType(),True),
        StructField("state_UF",StringType(),True),
        StructField("status",StringType(),True),
        StructField("subscription_term",StringType(),True),
        StructField("uid",StringType(),True),
        StructField("updatedAt",StringType(),True),
        StructField("user_name",StringType(),True) 
        ])

    #reading json files
    df_subscription = spark.read \
        .format("json") \
        .schema(customSchema) \
        .option("header", "true") \
        .option("mergeSchema", "true") \
        .json(get_subscription_file)
    
    #show the schema
    #df_subscription.printSchema()
    
    # get number of partitions
    print(df_subscription.rdd.getNumPartitions())

    #create database first
    create_db = spark.sql("""CREATE DATABASE IF NOT EXISTS metastore;""");
    create_db.show()

    # create iceberg table
    create_device_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS db.metastore.brasilian_subscription
        (
            address string,
            birth_date string,
            createdAt string,
            gender string,
            id long,
            payment_method string,
            payment_term string,
            plan string,
            state_UF string,
            status string,
            subscription_term string,
            uid string,
            updatedAt string,
            user_name string
        )
        USING iceberg
    """)

    # reference to spark sql engine ????????????????????
    df_subscription.createOrReplaceTempView("SUBSCRIPTION")

    # insert into table using new v2 dataframe write api
    spark.table("SUBSCRIPTION").writeTo("db.metastore.brasilian_subscription").append()

    # select data & query data  ????????????????????
    query_subscription = spark.table("db.metastore.brasilian_subscription")

    # amount of records written
    # 400
    print(query_subscription.count())

    # [udf] in python
    # business transformations
    def subscription_sla(subscription_plan):
        if subscription_plan in ("Business", "Diamond", "Gold", "Platinum", "Premium"):
            return "High"
        if subscription_plan in ("Bronze", "Essential", "Professional", "Silver", "Standard"):
            return "Normal"
        else:
            return "Low"

    # register function into spark's engine to make it available
    # once registered you can access in any language
    spark.udf.register("fn_subscription_sla", subscription_sla)

    # select columns of subscription
    # use alias to save your upfront process ~ silver
    # better name understanding for the business
    enhance_column_selection_subscription = df_subscription.select(
        col("ID").alias("SUBSCRIPTION_ID"),
        col("UID").alias("SUBSCRIPTION_UID"),
        col("PLAN").alias("SUBSCRIPTION_PLAN"),
        col("PAYMENT_TERM").alias("SUBSCRIPTION_PAYMENT_TERM"),
        col("PAYMENT_METHOD").alias("SUBSCRIPTION_PAYMENT_METHOD"),
        col("SUBSCRIPTION_TERM"),
        col("STATUS").alias("SUBSCRIPTION_STATUS"),
        col("USER_NAME"),col("BIRTH_DATE"),col("GENDER"),col("USER_NAME"),
        col("ADDRESS"),col("STATE_UF"),
        col("CREATEDAT"),col("UPDATEDAT")
        )

    # register as a spark sql object
    enhance_column_selection_subscription.createOrReplaceTempView("vw_subscription")

    # build another way to create functions
    # using spark sql engine capability to perform a case when
    # save the sql into a dataframe
    enhance_column_selection_subscription = spark.sql("""
    SELECT 
        SUBSCRIPTION_ID, SUBSCRIPTION_UID, 
        SUBSCRIPTION_PLAN, 
        fn_subscription_sla(SUBSCRIPTION_PLAN) AS SUBSCRIPTION_SLA,
        CASE WHEN subscription_plan = 'Basic' THEN 6.00 
            WHEN subscription_plan = 'Bronze' THEN 8.00 
            WHEN subscription_plan = 'Business' THEN 10.00 
            WHEN subscription_plan = 'Diamond' THEN 14.00
            WHEN subscription_plan = 'Essential' THEN 9.00 
            WHEN subscription_plan = 'Free Trial' THEN 0.00
            WHEN subscription_plan = 'Gold' THEN 25.00
            WHEN subscription_plan = 'Platinum' THEN 9.00
            WHEN subscription_plan = 'Premium' THEN 13.00
            WHEN subscription_plan = 'Professional' THEN 17.00
            WHEN subscription_plan = 'Silver' THEN 11.00
            WHEN subscription_plan = 'Standard' THEN 13.00
            WHEN subscription_plan = 'Starter' THEN 5.00
            WHEN subscription_plan = 'Student' THEN 2.00
        ELSE 0.00 
        END AS SUBSCRIPTION_PRICE,
        SUBSCRIPTION_STATUS,
        SUBSCRIPTION_PAYMENT_TERM, SUBSCRIPTION_PAYMENT_METHOD, SUBSCRIPTION_TERM
        USER_NAME, BIRTH_DATE, GENDER, 
        ADDRESS, STATE_UF, CREATEDAT, UPDATEDAT
    FROM vw_subscription
    """)

    # show & count df
    enhance_column_selection_subscription.explain()
    enhance_column_selection_subscription.count()

    # show df
    enhance_column_selection_subscription.show(5)
    
    
    # select columns for analysis
    # latest version
    select_columns_subscriptions = enhance_column_selection_subscription.select("SUBSCRIPTION_PLAN", "SUBSCRIPTION_PRICE", "SUBSCRIPTION_SLA", "STATE_UF")
    select_columns_subscriptions.show(5)
    
    
    # add timestamp column
    # generated column into df
    get_correct_columns = select_columns_subscriptions.select(
        col("SUBSCRIPTION_PLAN").alias("PLAN"),
        col("SUBSCRIPTION_PRICE").alias("PRICE"),
        col("SUBSCRIPTION_SLA").alias("SLA"),
        col("STATE_UF")
    )

    
    # create new iceberg table
    create_plans_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS db.metastore.brasilian_subscription_plans
        (
            PLAN string,
            PRICE decimal(4,2),
            SLA string,
            USER_AMOUNT integer,
            STATE_UF string
        )
        USING iceberg
    """)

    # insert into table 
    get_correct_columns.createOrReplaceTempView("VW_SUBSCRIPTION")
    
    get_aggregate = spark.sql(""" 
    SELECT 
        PLAN,
        PRICE,
        SLA,
        COUNT(PLAN) USER_AMOUNT,
        STATE_UF
    FROM 
        VW_SUBSCRIPTION
    GROUP BY PLAN,
             PRICE,
             SLA,
             STATE_UF
    ORDER BY PLAN
    """)

    get_aggregate.createOrReplaceTempView("VW_PLANS")

    spark.table("VW_PLANS").writeTo("db.metastore.brasilian_subscription_plans").append()
    
    
   

    # stop session
    spark.stop()
