def validate_orders(df):
    return df.filter(
        (df.customer_id.isNotNull()) &
        (df.product_id.isNotNull()) &
        (df.quantity > 0)
    )
