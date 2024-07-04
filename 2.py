from pyspark.sql import DataFrame, functions as F

def filter_and_modify_df(udf_df: DataFrame, primary_data_df: DataFrame) -> DataFrame:
    """
    Filters the UDF DataFrame based on primary data and adds a pivot column.

    :param udf_df: DataFrame containing UDF data.
    :param primary_data_df: DataFrame containing RR_ID/Versions for this run.
    :return: Filtered DataFrame with added pivot column.
    """
    
    return udf_df.join(primary_data_df, ["case_rk", "case_id"]).withColumn(
        "pivot_col", F.concat_ws("|", F.col("UDF_CAT"), F.col("UDF_KEY"))
    )
