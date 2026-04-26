from datetime import date
from typing import Deque

from pyspark.sql import DataFrame, SparkSession
from pydeequ.verification import VerificationSuite

import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.sql.window import Window

from .stocks_table import CuratedStocksTable
from ..raw.stocks_table import RawStocksTable

from pipelines import spark_client, SparkReader, SparkWriter, DeequRunner
from pipelines.models import (
    WriterConfig,
    IcebergTableConfig,
    EnumIngestionMode,
    EnumMergeStrategy,
    EnumReadMode,
    ReaderConfig,
    DeequConfig,
    DeequCheckConfig,
)


class CuratedStocks:
    def __init__(self, date: date = date.today()):
        self.date = date
        self.source = RawStocksTable()
        self.table = CuratedStocksTable()
        self.spark: SparkSession = spark_client.get_session()
        self.reader = SparkReader()
        self.writer = SparkWriter()

    def get_data(self):
        return self.reader.get_data(
            ReaderConfig(
                source_table=self.source.full_name(),
                target_table=self.table.full_name(),
                date_column="datetime",
                mode=EnumReadMode.INCREMENTAL,
            )
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

    def validate_data(self, df: DataFrame) -> DataFrame:
        runner = DeequRunner(self.spark)

        config = DeequConfig(
            checks=[
                DeequCheckConfig(column="symbol", check_type="completeness"),
                DeequCheckConfig(column="close", check_type="min", value=0),
            ]
        )

        result = runner.run_checks(df, config)

        return df

    def save(self, stocks: DataFrame) -> None:
        self.writer.write_data(
            stocks,
            WriterConfig(
                iceberg_table_cfg=IcebergTableConfig(
                    table_name=self.table.full_name(),
                    schema=self.table.schema(),
                    partitions=["symbol", "datetime"],
                ),
                mode=EnumIngestionMode.UPSERT,
                strategy=EnumMergeStrategy.TYPE1,
                merge_columns=["symbol", "datetime"],
            ),
        )

    def run(self) -> None:
        stocks = self.get_data()
        stocks = self.cast_columns(stocks=stocks)
        stocks = self.metric_columns(stocks=stocks)
        stocks = self.validate_data(stocks)
        self.save(stocks)
