-----Step 1: Join Sales + Customers + Products

import dlt
from pyspark.sql.functions import col, sum as _sum, window

#  Enriched Sales Fact
@dlt.table(name="business_sales_enriched")
def business_sales_enriched():
    sales = spark.readStream.table("sales_stream").alias("s")
    customers = spark.readStream.table("customers_stream").alias("c")
    products = spark.readStream.table("products_stream").alias("p")

    df = (
        sales
        .join(customers, col("s.customer_id") == col("c.customer_id"), "inner")
        .join(products, col("s.product_id") == col("p.product_id"), "inner")
        .select(
            col("s.sales_id"),
            col("s.customer_id").alias("customer_id"),
            col("s.product_id").alias("product_id"),
            col("s.total"),
            col("s.sale_timestamp"),
            col("c.customer_name"),
            col("c.region"),
            col("p.category")
        )
    )
    return df

#  STREAMING AGGREGATES (for ML / raw analytics)
@dlt.table(name="sales_by_region")
def sales_by_region():
    df = spark.readStream.table("business_sales_enriched")
    return df.groupBy(
        "region",
        window("sale_timestamp", "30 days")
    ).agg(
        _sum("total").alias("total_sales")
    )

@dlt.table(name="sales_by_product_category")
def sales_by_product_category():
    df = spark.readStream.table("business_sales_enriched")
    return df.groupBy(
        "category",
        window("sale_timestamp", "30 days")
    ).agg(
        _sum("total").alias("total_sales")
    )

@dlt.table(name="top_customers")
def top_customers():
    df = spark.readStream.table("business_sales_enriched")
    return df.groupBy(
        "customer_id", "customer_name",
        window("sale_timestamp", "30 days")
    ).agg(
        _sum("total").alias("total_spent")
    ).orderBy(col("total_spent").desc())
