from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, expr, row_number, collect_list, struct
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("SplitBySize").getOrCreate()

# Sample DataFrame
data = [
    ({"financialInstitutionCode": "AFS", "partySourceSysCode": "FN", "partyLocalId": "110000012"},
     [{"dataSourceCode": "SAS", "inheritedRiskRating": "L", "inheritedRiskRatingDesc": "Low", "recordStartTs": "2024-07-12T14:47:15.135882+10:00", "recordEndTs": "2024-07-12T14:47:15.135882+10:00"}] * 1000)
]

# Schema
schema = StructType([
    StructField("partyIdentifier", MapType(StringType(), StringType()), True),
    StructField("fcCore", ArrayType(StructType([
        StructField("dataSourceCode", StringType(), True),
        StructField("inheritedRiskRating", StringType(), True),
        StructField("inheritedRiskRatingDesc", StringType(), True),
        StructField("recordStartTs", StringType(), True),
        StructField("recordEndTs", StringType(), True)
    ])), True)
])

df = spark.createDataFrame(data, schema=schema)

# Explode fcCore array to individual rows
df_exploded = df.withColumn("fcCore", explode("fcCore"))

# Add size column (assuming a fixed size for demonstration)
df_with_size = df_exploded.withColumn("size", lit(1024))  # Assuming each entry is 1 KB for this example

# Calculate cumulative size and rank
window_spec = Window.partitionBy("partyIdentifier").orderBy("recordStartTs")
df_with_cumulative_size = df_with_size.withColumn("cumulative_size", sum("size").over(window_spec))

# Add rank based on 10 MB chunks
df_with_rank = df_with_cumulative_size.withColumn("rank", (col("cumulative_size") / (10 * 1024)).cast("integer") + 1)

# Aggregate back to arrays and prepare payload
payload_df = df_with_rank.groupBy("partyIdentifier", "rank").agg(
    collect_list(struct("fcCore.*")).alias("fcCore")
)

# Show the result
payload_df.show(truncate=False)
