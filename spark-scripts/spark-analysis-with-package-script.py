import os
import pyspark
from pyspark.sql.functions import count, sum, avg, col, when, rank, collect_set
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.fpm import FPGrowth

# Retrieve env info for postgres
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD") 
POSTGRES_HOST = os.environ.get("POSTGRES_CONTAINER_NAME")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DW_DB")

print("environtment pg: ")
print(POSTGRES_USER)
print(POSTGRES_PASSWORD)
print(POSTGRES_HOST)
print(POSTGRES_PORT)
print(POSTGRES_DATABASE)


# Create SparkSession
spark_context = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Batch_Processing')
        .setMaster('local')
    ))
spark_context.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(spark_context.getOrCreate())

# Construct the PostgreSQL JDBC URL using environment variables
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
jdbc_properties = {
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Read data from PostgreSQL 
try:
    df = spark.read.jdbc(
        jdbc_url,
        'public.retail',
        properties=jdbc_properties
    )
except Exception as e:
    print(f"Error reading data from PostgreSQL: {e}")
    raise

# Data Cleaning
# Handle nulls and non-numeric values
df = df.withColumn("quantity", when(col("quantity").isNull() | 
                                    ~col("quantity").rlike("^[0-9]*$"), 0) 
                             .otherwise(col("quantity"))) \
        .withColumn("unitprice", when(col("unitprice").isNull() | 
                                      ~col("unitprice").rlike("^[0-9]*(\.[0-9]+)?$"), 0.0) 
                               .otherwise(col("unitprice")))

# Convert columns to numeric types
df = df.withColumn("quantity", col("quantity").cast(IntegerType())) \
        .withColumn("unitprice", col("unitprice").cast(DoubleType()))

# Fill nulls with 0
df = df.fillna(0, subset=["quantity", "unitprice"])

# Data analysis
# 1. Simple Aggregation
agg_df = df.groupBy("country").agg(
    count("invoiceno").alias("total_invoices"),
    sum("quantity").alias("total_quantity"),
    avg("unitprice").alias("avg_unitprice")
)

# 2. Product Popularity by Country
product_popularity_df = df.groupBy("country", "stockcode") \
    .agg(sum("quantity").alias("total_quantity_sold")) \
    .withColumn("rank", rank().over(Window.partitionBy("country").orderBy(col("total_quantity_sold").desc()))) \
    .filter(col("rank") <= 3) 

# 3. Product Affinity Analysis
# Prepare data for product affinity analysis
basket_data = df.groupBy("invoiceno").agg(collect_set("stockcode").alias("items"))

# Apply Frequent Pattern Growth (FP-Growth) algorithm
fpGrowth = FPGrowth(itemsCol="items", minSupport=0.05, minConfidence=0.5)
model = fpGrowth.fit(basket_data)

# Write results to PostgreSQL
agg_df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "agg_results")  \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

product_popularity_df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "product_popularity_results") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

model.freqItemsets.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "freq_itemsets") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Stop SparkSession
spark.stop()