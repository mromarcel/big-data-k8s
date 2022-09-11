# import libraries
from delta import *
from delta.tables import *
import pytz
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import DateType, StringType, TimestampType, DecimalType, LongType, StructField, StructType

from pyspark.sql.functions import current_timestamp, col



# init application
if __name__ == '__main__':

    # init session and setting configs
    spark = SparkSession \
        .builder \
        .appName("spark-app-brazilian-subscription-delta") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio.deepstorage.svc.cluster.local:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "myaccesskey") \
        .config("spark.hadoop.fs.s3a.secret.key", "mysecretkey") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.fast.upload", True) \
        .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
        .config("fs.s3a.connection.maximum", 100) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .config("hive.metastore.uris", "thrift://hive.metastore.svc.cluster.local:9083") \
        .getOrCreate()
    
    # show all parameters
    print(SparkConf().getAll())

    # set log level
    # ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    spark.sparkContext.setLogLevel("FATAL")

    # Get the past 1 hours interval 
    # PAST_HOUR_PREFIX = (datetime.now(pytz.timezone('America/Sao_Paulo'))-timedelta(hours=1) ).strftime("%Y/%m/%d/%H")
    CURRENT_HOUR_PREFIX = (datetime.now(pytz.timezone('America/Sao_Paulo')) ).strftime("%Y/%m/%d/%H")

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

    # count amount of rows ingested from lake
    df_subscription.count()
    df_subscription = df_subscription.withColumn("BRONZE_EVENT_TIME", current_timestamp())
    df_subscription.show()

    write_delta_mode = "overwrite"
    delta_bronze_zone = "s3a://deltalake/bronze"
    df_subscription.write.mode(write_delta_mode).format("delta").save(delta_bronze_zone + "/brazilian-subscriptions/")

    # [silver zone area]
    # applying enrichment

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
        col("CREATEDAT"),col("UPDATEDAT"),
        col("BRONZE_EVENT_TIME")
        )
        

    # register as a spark sql object
    enhance_column_selection_subscription.createOrReplaceTempView("VW_SUBSCRIPTION")
    # spark.sql(""" select * from VW_SUBSCRIPTION limit 2""").show()

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
        ADDRESS, STATE_UF, CREATEDAT, UPDATEDAT,
        BRONZE_EVENT_TIME
    FROM vw_subscription
    """)

    # show & count df
    enhance_column_selection_subscription.explain()
    enhance_column_selection_subscription.count()

    # show the first rows 
    enhance_column_selection_subscription = enhance_column_selection_subscription.withColumn("SILVER_EVENT_TIME", current_timestamp())
    enhance_column_selection_subscription.show(5)

    write_delta_mode = "overwrite"
    delta_silver_zone = "s3a://deltalake/silver"
    enhance_column_selection_subscription.write.format("delta").mode(write_delta_mode).save(delta_silver_zone + "/brazilian-subscriptions/")
    
    # read delta table
    # new feature of delta 1.0.0
    # show info
    # toDF() = dataframe representation
    # of a delta table
    delta_lake_location_subscriptions = "s3a://deltalake/silver/brazilian-subscriptions"
    dt_subscriptions = DeltaTable.forPath(spark, delta_lake_location_subscriptions)
    dt_subscriptions.toDF().show(5)

    # select columns for analysis
    # latest version
    select_columns_subscriptions = dt_subscriptions.toDF().select("SUBSCRIPTION_PLAN", "SUBSCRIPTION_PRICE", "SUBSCRIPTION_SLA", "GENDER", "STATE_UF","BRONZE_EVENT_TIME","SILVER_EVENT_TIME")
    select_columns_subscriptions.show()

    # add timestamp column
    # generated column into df
    get_plans_df = select_columns_subscriptions.withColumn("GOLD_EVENT_TIME", current_timestamp())
    get_correct_columns = get_plans_df.select(
        col("SUBSCRIPTION_PLAN").alias("PLAN"),
        col("SUBSCRIPTION_PRICE").alias("PRICE"),
        col("SUBSCRIPTION_SLA").alias("SLA"),
        col("BRONZE_EVENT_TIME"),
        col("SILVER_EVENT_TIME"),
        col("GOLD_EVENT_TIME")
    )

    # building [gold zone area]
    get_correct_columns.createOrReplaceTempView("VW_PLANS")

    get_correct_columns = spark.sql(""" 
    SELECT 
        PLAN,
        PRICE,
        SLA,
        COUNT(PLAN) AMOUNT,
        MAX(BRONZE_EVENT_TIME) AS BRONZE_EVENT_TIME,
        MAX(SILVER_EVENT_TIME) AS SILVER_EVENT_TIME,
        MAX(GOLD_EVENT_TIME) AS GOLD_EVENT_TIME
    FROM 
        VW_PLANS
    GROUP BY PLAN,
             PRICE,
             SLA
    ORDER BY PLAN
    """)

    # create table in metastore
    # deltatablebuilder api
    delta_gold_tb_plans_location = "s3a://deltalake/gold/brazilian-subscriptions/plans"
    DeltaTable.createIfNotExists(spark) \
        .tableName("PLANS") \
        .addColumn("PLAN", StringType()) \
        .addColumn("PRICE", DecimalType(4, 2)) \
        .addColumn("SLA", StringType()) \
        .addColumn("USER_AMOUNT", LongType()) \
        .addColumn("BRONZE_EVENT_TIME", TimestampType()) \
        .addColumn("SILVER_EVENT_TIME", TimestampType()) \
        .addColumn("GOLD_EVENT_TIME", TimestampType()) \
        .addColumn("DATE", DateType(), generatedAlwaysAs="CAST(GOLD_EVENT_TIME AS DATE)") \
        .partitionedBy("DATE") \
        .location(delta_gold_tb_plans_location) \
        .execute()

    # insert into new table
    get_correct_columns.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_gold_tb_plans_location)

    # delta table name = plans
    # read latest rows added
    dt_plans_delta = DeltaTable.forPath(spark, delta_gold_tb_plans_location)
    dt_plans_delta.toDF().show(5)

    # describe history
    # find timestamp and version
    dt_subscriptions.history().show()
    dt_plans_delta.history().show()

    # count rows
    # current and previous version
    dt_subscriptions.toDF().count()

    # data retention
    # retain commit history for 30 days
    # not run VACUUM, if so, lose the ability to
    # go back older than 7 days
    # configure retention period
    # delta.logRetentionDuration = "interval <interval>":
    # delta.deletedFileRetentionDuration = "interval <interval>":
    dt_plans_delta.vacuum()

# stop session
spark.stop()



