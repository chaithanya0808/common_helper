from pyspark.sql.window import Window

# Calculate cumulative size per partyIdentifier
windowSpec = Window.partitionBy("partyIdentifier")
df_with_cumulative_size = df_with_sizes.withColumn("cumulative_size",
    F.sum("message_size").over(windowSpec)
)

# Determine split points based on 1 MB limit
df_with_split_flags = df_with_cumulative_size.withColumn("split_flag",
    F.when(F.col("cumulative_size") > 1024*1024, 1).otherwise(0)
)

# Assign a message number to each split
df_with_messages = df_with_split_flags.withColumn("message_number",
    F.sum("split_flag").over(windowSpec.orderBy("timestamp"))
)

# Extract messages based on message_number
messages = df_with_messages.groupBy("partyIdentifier", "message_number").agg(
    F.collect_list(F.struct(*df.columns)).alias("data")
).orderBy("partyIdentifier", "message_number")

# Convert each message to a JSON string and send to Kafka
for message in messages.collect():
    json_message = [row.asDict() for row in message.data]
    json_message_str = json.dumps(json_message)
    # Send json_message_str to Kafka
    # kafka_producer.send(topic, json_message_str)
