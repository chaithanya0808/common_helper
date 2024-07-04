from pyspark.sql import DataFrame

def process_udf_data(query_path: str, 
                     date_udf_sql_path: str, 
                     desc_req: bool = False, 
                     desc_query_path: str = None) -> DataFrame:
    """
    Processes UDF data by reading SQL queries, generating UDF DataFrames, and combining them.

    :param query_path: Path to the primary SQL query file.
    :param date_udf_sql_path: Path to the date UDF SQL query file.
    :param desc_req: Boolean flag to include description UDF data.
    :param desc_query_path: Path to the description UDF SQL query file (optional).
    :return: Combined DataFrame with UDF data.
    """
    
    query = read_file_content(query_path)
    df = gen_udf_df(query)
    
    date_udf_sql = read_file_content(date_udf_sql_path)
    df = df.union(gen_udf_df(date_udf_sql).transform(to_custom_timestamp))
    
    if desc_req and desc_query_path:
        desc_query = read_file_content(desc_query_path)
        df = df.union(gen_udf_df(desc_query))
    
    return df
