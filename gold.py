from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, avg, count

@dp.table(
    name="gold_fact_ordersv1",
    comment="Gold fact table for orders analysis"
)
def gold_fact_orders():
    orders = spark.read.table("silver_ordersv1")

    return (
        orders.select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "order_year",
            "order_month",
            "quantity",
            "total_amount",
            "discounted_price",
            "flag"
        )
    )
	
	
@dp.table(
    name="gold_fact_revenue_monthlyv1",
    comment="Monthly revenue and discount analysis"
)
def gold_fact_revenue():
    orders = spark.read.table("silver_ordersv1")

    return (
        orders.groupBy("order_year", "order_month")
        .agg(
            count("*").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            sum(col("total_amount") - col("discounted_price")).alias("total_discount"),
            avg("total_amount").alias("avg_order_value")
        )
    )
	
	
@dp.table(
    name="gold_fact_product_salesv1",
    comment="Product performance analytics"
)
def gold_fact_product_sales():
    orders = spark.read.table("silver_ordersv1")
    products = spark.read.table("silver_productsv1")

    return (
        orders.join(products, "product_id", "left")
        .groupBy(
            "product_id",
            "product_name",
            "category",
            "brand"
        )
        .agg(
            sum("quantity").alias("total_quantity_sold"),
            sum("total_amount").alias("total_revenue"),
            avg("price").alias("avg_price")
        )
    )
	
	
@dp.table(
    name="gold_dim_customersv1",
    comment="Customer dimension for analytics"
)
def gold_dim_customers():
    return (
        spark.read.table("silver_customersv1")
        .select(
            "customer_id",
            "full_name",
            "email",
            "domain",
            "city",
            "state"
        )
    )
	
	
@dp.table(
    name="gold_dim_productsv1",
    comment="Product dimension for analytics"
)
def gold_dim_products():
    return (
        spark.read.table("silver_productsv1")
        .select(
            "product_id",
            "product_name",
            "category",
            "brand",
            "price",
            "discount_price"
        )
    )


@dp.table(
    name="gold_dim_regionsv1",
    comment="Region dimension table"
)
def gold_dim_regions():
    return spark.read.table("silver_regionv1")
	
	


from pyspark.sql.functions import expr, year, month, dayofmonth	
@dp.table(
    name="gold_top_customersv1",
    comment="Top customers by revenue"
)
def gold_top_customers():
    orders = spark.read.table("silver_ordersv1")
    customers = spark.read.table("silver_customersv1")

    return (
        orders.join(customers, "customer_id")
        .groupBy("customer_id", "full_name", "domain")
        .agg(
            sum("total_amount").alias("total_spent"),
            count("*").alias("total_orders")
        )
        .orderBy(col("total_spent").desc())
    )
	

@dp.table(
    name="gold_top_productsv1",
    comment="Top products by revenue"
)
def gold_top_products():
    orders = spark.read.table("silver_ordersv1")
    products = spark.read.table("silver_productsv1")

    return (
        orders.join(products, "product_id")
        .groupBy("product_name", "category")
        .agg(
            sum("quantity").alias("total_quantity"),
            sum("total_amount").alias("total_revenue")
        )
        .orderBy(col("total_revenue").desc())
    )
	
	


@dp.table(
    name="gold_region_salesv1",
    comment="Region-wise sales performance"
)
def gold_region_sales():
    orders = spark.read.table("silver_ordersv1")
    customers = spark.read.table("silver_customersv1")
    regions = spark.read.table("silver_regionv1")

    return (
        orders.join(customers, "customer_id")
        .join(regions, customers.city == regions.region, "left")
        .groupBy("region")
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("*").alias("total_orders")
        )
    )
	
	
