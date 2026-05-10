from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    upper,
    round,
    current_timestamp
)


class SilverProductsTransformer:

    def __init__(self, df):
        self.df = df

    def drop_rescued_column(self):
        if "_rescued_data" in self.df.columns:
            self.df = self.df.drop("_rescued_data")

        if "rescued_data" in self.df.columns:
            self.df = self.df.drop("rescued_data")

        return self

    def add_discount_price(self):
        self.df = self.df.withColumn(
            "discount_price",
            round(col("price") * 0.90, 2)
        )
        return self

    def convert_brand_uppercase(self):
        self.df = self.df.withColumn(
            "brand",
            upper(col("brand"))
        )
        return self

    def add_audit_columns(self):
        self.df = self.df.withColumn(
            "silver_updated_time",
            current_timestamp()
        )
        return self

    def transform(self):
        return (
            self.drop_rescued_column()
            .add_discount_price()
            .convert_brand_uppercase()
            .add_audit_columns()
            .df
        )


@dp.table(
    name="silver_productsv1",
    comment="Silver products table with 10 percent discount price and uppercase brand"
)
def silver_products():
    bronze_df = spark.read.table("bronze_productsv1")

    return SilverProductsTransformer(bronze_df).transform()