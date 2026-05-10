from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    split,
    lower,
    trim,
    concat_ws,
    current_timestamp,
    count
)


class SilverCustomersTransformer:

    def __init__(self, df):
        self.df = df

    def drop_rescued_column(self):
        if "_rescued_data" in self.df.columns:
            self.df = self.df.drop("_rescued_data")

        if "rescued_data" in self.df.columns:
            self.df = self.df.drop("rescued_data")

        return self

    def add_domain_column(self):
        self.df = self.df.withColumn(
            "domain",
            lower(trim(split(col("email"), "@").getItem(1)))
        )
        return self

    def add_full_name_column(self):
        self.df = self.df.withColumn(
            "full_name",
            concat_ws(" ", col("first_name"), col("last_name"))
        )

        self.df = self.df.drop("first_name", "last_name")
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
            .add_domain_column()
            .add_full_name_column()
            .add_audit_columns()
            .df
        )


@dp.table(
    name="silver_customersv1",
    comment="Silver customer table with domain and full_name"
)
def silver_customers():
    bronze_df = spark.read.table("bronze_customersv1")

    return SilverCustomersTransformer(bronze_df).transform()


@dp.table(
    name="silver_customer_domain_countv1",
    comment="Customer count by email domain sorted ascending"
)
def silver_customer_domain_count():

    df = spark.read.table("silver_customersv1")

    return (
        df.groupBy("domain")
        .agg(count("*").alias("total_customers"))
        .orderBy(col("total_customers").asc())
    )


@dp.table(
    name="silver_customers_gmailv1",
    comment="Customers having gmail.com domain"
)
def silver_customers_gmail():
    return (
        spark.read.table("silver_customersv1")
        .filter(col("domain") == "gmail.com")
    )


@dp.table(
    name="silver_customers_yahoov1",
    comment="Customers having yahoo.com domain"
)
def silver_customers_yahoo():
    return (
        spark.read.table("silver_customersv1")
        .filter(col("domain") == "yahoo.com")
    )


@dp.table(
    name="silver_customers_hotmailv1",
    comment="Customers having hotmail.com domain"
)
def silver_customers_hotmail():
    return (
        spark.read.table("silver_customersv1")
        .filter(col("domain") == "hotmail.com")
    )