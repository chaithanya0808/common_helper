from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, collect_list, length, expr, row_number, sum as _sum
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("ProcessLargeEntries").getOrCreate()

# Sample data
data = [
    {
        "partyIdentifier": {
            "financialInstitutionCode": "AFS",
            "partySourceSysCode": "FN",
            "partyLocalId": "110000012"
        },
        "fcCore": [
            {"dataSourceCode": "SAS", "inheritedRiskRating": "L", "inheritedRiskRatingDesc": "Low", "recordStartTs": "2024-07-12T14:47:15.135882+10:00", "recordEndTs": "2024-07-12T14:47:15.135882+10:00"},
            {"dataSourceCode": "SAS", "inheritedRiskRating": "M", "inheritedRiskRatingDesc": "Medium", "recordStartTs": "2024-07-12T14:47:15.135882+10:00", "recordEndTs": "2024-07-12T14:47:15.135882+10:00"},
            {"dataSourceCode": "SAS", "inheritedRiskRating": "H", "inheritedRiskRatingDesc": "High", "recordStartTs": "2024-07-12T14:47:15.135882+10:00", "recordEndTs": "2024-07-12T14:47:15.135882+10:00"},
            {"dataSourceCode": "SAS", "inheritedRiskRating": "L", "inheritedRiskRatingDesc": "Low", "recordStartTs": "2024-07-12T14:47:15.135882+10:00", "recordEndTs": "2024-07-12T14:47:15.135882+10:00"}
        ],
        "metadata": {
            "dataSource": "SAS",
            "source": "FCDP"
        }
    }
]

# Create DataFrame
df = spark.read.json(spark.sparkContext.parallelize(data))

# Convert partyIdentifier and metadata to JSON strings to calculate their sizes
df = df.withColumn("partyIdentifier_json", expr("to_json(partyIdentifier)"))
df = df.withColumn("metadata_json", expr("to_json(metadata)"))
df = df.withColumn("partyIdentifier_size", length(col("partyIdentifier_json")).cast(IntegerType()))
df = df.withColumn("metadata_size", length(col("metadata_json")).cast(IntegerType()))

# Explode fcCore array into individual rows
df_exploded = df.withColumn("fcCore", explode("fcCore"))

# Convert each fcCore entry to a JSON string and calculate its size
df_exploded = df_exploded.withColumn("fcCore_json", expr("to_json(fcCore)"))
df_exploded = df_exploded.withColumn("fcCore_size", length(col("fcCore_json")).cast(IntegerType()))

# Define window specification
window_spec = Window.partitionBy("partyIdentifier").orderBy("fcCore.recordStartTs")

# Add cumulative size including partyIdentifier and metadata
df_exploded = df_exploded.withColumn("cumulative_size", 
    _sum("fcCore_size").over(window_spec) + 
    col("partyIdentifier_size").cast(IntegerType()) + 
    col("metadata_size").cast(IntegerType()))

# Assign rank based on cumulative size exceeding 1 MB
df_exploded = df_exploded.withColumn("chunk",
    (df_exploded.cumulative_size / (1024 * 1024)).cast("int"))

# Add row number within each chunk to order by
window_spec_chunk = Window.partitionBy("partyIdentifier", "chunk").orderBy("fcCore.recordStartTs")
df_exploded = df_exploded.withColumn("row_number", row_number().over(window_spec_chunk))

# Group by partyIdentifier and chunk, then collect the entries
df_grouped = df_exploded.groupBy("partyIdentifier", "chunk").agg(
    collect_list("fcCore").alias("fcCore"))

# Add metadata back to each group
metadata = data[0]["metadata"]
df_grouped = df_grouped.withColumn("metadata", lit(metadata))

# Select necessary columns and convert to desired format
result_df = df_grouped.select("partyIdentifier", "metadata", "fcCore")

# Order by partyIdentifier and chunk
result_df = result_df.orderBy("partyIdentifier", "chunk")

result_df.show(truncate=False)
