from datetime import datetime, date
from typing import Dict, Any

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from clients.crypto_stocks_client import CryptoStocksClient
from clients.spark_client import spark_client

from .stocks_table import RawStocksTable


class RawStocksJob:
    def __init__(self, date: date = date.today()):
        self.raw_table = RawStocksTable()
        self.date = date
        self.client = CryptoStocksClient()
        self.spark: SparkSession = spark_client.get_session()
        self.stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"]

    def _get_data(self) -> list[str]:
        return self.client.get_stocks(date=self.date, stocks_symbol=self.stocks)

    def _clean_data(self) -> list[dict]:
        data = self._get_data()
        print(data)

        records = []

        for symbol, stock_data in data.items():
            values = stock_data.get("values", [])

            for row in values:
                records.append(
                    {
                        "symbol": symbol,
                        "datetime": row.get("datetime"),
                        "open": row.get("open"),
                        "high": row.get("high"),
                        "low": row.get("low"),
                        "close": row.get("close"),
                        "volume": row.get("volume"),
                    }
                )

        return records

    def create_dataframe(self) -> DataFrame:
        stocks_data = self._clean_data()

        stocks_df: DataFrame = self.spark.createDataFrame(
            stocks_data, schema=self.raw_table.schema()
        )

        stocks_df = stocks_df.withColumn("dt_reference", F.lit(self.date))

        return stocks_df

    def create_table(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.raw_table.full_name()} (
                symbol STRING,
                datetime STRING,
                open STRING,
                high STRING,
                low STRING,
                close STRING,
                volume STRING,
                dt_reference DATE
            )
            USING iceberg
            PARTITIONED BY (dt_reference)
        """)

    def save(self, df: DataFrame) -> None:
        df.writeTo(self.raw_table.full_name()).overwritePartitions()

    def run(self):
        stocks = self.create_dataframe()
        self.create_table()
        self.save(stocks)
        
        stocks.show()
