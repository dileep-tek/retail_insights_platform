from pyspark.sql.functions import col

def transform_data(orders, customers, products):

    df = orders.join(customers, "customer_id", "inner") \
               .join(products, "product_id", "inner")

    df = df.withColumn("total_amount", col("price") * col("quantity"))

    return df
