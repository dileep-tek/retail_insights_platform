from pyspark.sql.functions import col

def incremental_load(df, last_processed_date):
    return df.filter(col("updated_at") > last_processed_date)
