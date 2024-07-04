from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def udf_table(primary_data_df: DataFrame, query: str, date_udf_query: str, desc_req: bool = False, desc_query: str = None) -> DataFrame:
    """
    This function extracts udf data from SAS for this iteration.

    :param primary_data_df: The dataframe having RR_ID/Versions for this run.
    :param query: The main query to be executed.
    :param date_udf_query: The query to be used for generating date UDF data.
    :param desc_req: Boolean flag to indicate if descriptive query is required.
    :param desc_query: The descriptive query to be executed if desc_req is True.
    :return: This function extracts udf data from SAS for this iteration and
    returns a dataframe with an added column to pivot against.
    """
    
    df = gen_udf_df(query)
    df = df.union(gen_udf_df(date_udf_query).transform(to_custom_timestamp))

    if desc_req and desc_query:
        df = df.union(gen_udf_df(desc_query))
    
    # Filter based on primary data
    return df.join(primary_data_df, ["case_rk", "case_id"]).withColumn(
        "pivot_col", F.concat_ws("|", "UDF_CAT", "UDF_KEY")
    )
# usage:
# primary_data_df = ...  # Your DataFrame
# query = read_file_content("path/path.sql")
# date_udf_query = read_file_content("path1/path1.sql")
# desc_query = read_file_content("path2/path2.sql")

# result_df = udf_table(primary_data_df, query, date_udf_query, desc_req=True, desc_query=desc_query)
