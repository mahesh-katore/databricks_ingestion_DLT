from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    upper,
    round,
    current_timestamp
)


class SilverregionTransformer:

    def __init__(self, df):
        self.df = df

    def drop_rescued_column(self):
        if "_rescued_data" in self.df.columns:
            self.df = self.df.drop("_rescued_data")

        if "rescued_data" in self.df.columns:
            self.df = self.df.drop("rescued_data")

        return self

    def transform(self):
        return (
            self.drop_rescued_column()
            .df
        )


@dp.table(
    name="silver_regionv1",
    comment="Silver region Selection"
)
def silver_region():
    bronze_df = spark.read.table("bronze_regionv1")

    return SilverregionTransformer(bronze_df).transform()