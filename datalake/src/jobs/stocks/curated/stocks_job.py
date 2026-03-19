from datetime import date

from pyspark.sql import DataFrame, SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.sql.window import Window

from .stocks_table import CuratedStocksTable
from ..raw.stocks_table import RawStocksTable
from clients.spark_client import spark_client


class CuratedStocks:
    def __init__(self, date: date = date.today()):
        self.date = date
        self.source = RawStocksTable()
        self.table = CuratedStocksTable()
        self.spark: SparkSession = spark_client.get_session()

    def get_data(self):
        return self.spark.read.table(self.source.full_name()).filter(
            F.col("dt_reference") == self.date
        )

    def cast_columns(self, stocks: DataFrame) -> DataFrame:
        stocks = stocks.select(
            F.col("symbol").cast(t.StringType()),
            F.col("datetime").cast(t.DateType()),
            F.col("open").cast(t.DoubleType()),
            F.col("high").cast(t.DoubleType()),
            F.col("low").cast(t.DoubleType()),
            F.col("close").cast(t.DoubleType()),
            F.col("volume").cast(t.LongType()),
            F.col("dt_reference").cast(t.DateType()),
        )

        return stocks

    def metric_columns(self, stocks: DataFrame) -> DataFrame:
        w = Window.partitionBy("symbol").orderBy("datetime")

        return (
            stocks.withColumn("price_variation", F.col("close") - F.col("open"))
            .withColumn("pct_variation", (F.col("close") / F.col("open") - 1) * 100)
            .withColumn("range", F.col("high") - F.col("low"))
            .withColumn("prev_close", F.lag("close").over(w))
            .withColumn(
                "daily_return", (F.col("close") / F.col("prev_close") - 1) * 100
            )
        )

    def save(self, stocks: DataFrame):
        spark_client.create_iceberg_table(
            table_name=self.table.full_name(),
            schema=self.table.schema(),
            partitions=["dt_reference"],
        )

        stocks.writeTo(self.table.full_name()).overwritePartitions()

    def run(self) -> None:
        stocks = self.get_data()
        stocks = self.cast_columns(stocks=stocks)
        stocks = self.metric_columns(stocks=stocks)
        self.save(stocks)

        stocks.show()
