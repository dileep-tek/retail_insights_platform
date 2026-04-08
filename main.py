from common.spark_session import get_spark
from common.validations import validate_orders
from jobs.incremental_load import incremental_load
from jobs.transform import transform_data
from jobs.aggregate import customer_metrics

def main():

    spark = get_spark()

    customers = spark.read.csv("data/raw/customers.csv", header=True, inferSchema=True)
    products = spark.read.csv("data/raw/products.csv", header=True, inferSchema=True)
    orders = spark.read.csv("data/raw/orders.csv", header=True, inferSchema=True)

    # Step 1: Validation
    orders = validate_orders(orders)

    # Step 2: Incremental Load
    orders = incremental_load(orders, "2024-01-11")

    # Step 3: Transform
    transformed = transform_data(orders, customers, products)

    # Step 4: Aggregation
    result = customer_metrics(transformed)

    result.show()

    result.write.mode("overwrite").csv("data/output/customer_metrics", header=True)

    spark.stop()

if __name__ == "__main__":
    main()
