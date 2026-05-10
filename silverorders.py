from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dense_rank,
    rank,
    row_number,
    when,
    current_timestamp
)
from pyspark.sql.window import Window


class SilverOrdersTransformer:

    def __init__(self, df):
        self.df = df

    def rename_rescued_column(self):
        if "_rescued_data" in self.df.columns:
            self.df = self.df.withColumnRenamed("_rescued_data", "rescued_data")
        return self

    def drop_rescued_column(self):
        if "rescued_data" in self.df.columns:
            self.df = self.df.drop("rescued_data")
        return self

    def convert_order_date(self):
        self.df = self.df.withColumn(
            "order_date_ts",
            to_timestamp(col("order_date"))
        )
        return self

    def add_year_month_columns(self):
        self.df = (
            self.df
            .withColumn("order_year", year(col("order_date_ts")))
            .withColumn("order_month", month(col("order_date_ts")))
        )
        return self

    def add_window_flags(self):
        monthly_window = Window.partitionBy(
            "order_year",
            "order_month"
        ).orderBy(
            col("total_amount").desc()
        )

        self.df = (
            self.df
            .withColumn("dense_rank_flag", dense_rank().over(monthly_window))
            .withColumn("rank_flag", rank().over(monthly_window))
            .withColumn("row_flag", row_number().over(monthly_window))
        )

        return self

    def add_business_flag(self):
        self.df = self.df.withColumn(
            "flag",
            when(col("dense_rank_flag") == 1, "TOP_ORDER")
            .when(col("dense_rank_flag") <= 3, "HIGH_ORDER")
            .otherwise("NORMAL_ORDER")
        )
        return self

    def add_audit_columns(self):
        self.df = self.df.withColumn(
            "silver_updated_time",
            current_timestamp()
        )
        return self

 
    def add_discounted_price(self):
        self.df = self.df.withColumn(
            "discounted_price",
            col("total_amount") * 0.90
        )
        return self

    def transform(self):
        return (
            self.rename_rescued_column()
            .drop_rescued_column()
            .convert_order_date()
            .add_year_month_columns()
            .add_window_flags()
            .add_business_flag()
            .add_audit_columns()
            .add_discounted_price()
            .df
        )


@dp.table(
    name="silver_ordersv1",
    comment="Silver orders table with timestamp conversion, year/month, and ranking flags"
)
def silver_orders():
    bronze_df = spark.read.table("bronze_ordersv1")

    silver_df = SilverOrdersTransformer(bronze_df).transform()

    return silver_df