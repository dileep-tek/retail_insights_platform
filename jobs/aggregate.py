from pyspark.sql.functions import sum as _sum, count

def customer_metrics(df):

    result = df.groupBy("customer_id", "name") \
        .agg(
            _sum("total_amount").alias("total_spent"),
            count("order_id").alias("total_orders")
        )

    return result
