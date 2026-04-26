from datetime import date

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from clients.crypto_stocks_client import CryptoStocksClient

from pipelines import spark_client, SparkWriter
from pipelines.models import WriterConfig, IcebergTableConfig, EnumIngestionMode

from .stocks_table import RawStocksTable


class RawStocksJob:
    def __init__(self, date: date = date.today()):
        self.raw_table = RawStocksTable()
        self.date = date
        self.client = CryptoStocksClient()
        self.spark: SparkSession = spark_client.get_session()
        self.writer = SparkWriter()
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

    def save(self, df: DataFrame) -> None:
        self.writer.write_data(
            df,
            WriterConfig(
                IcebergTableConfig(
                    table_name=self.raw_table.full_name(),
                    schema=self.raw_table.schema(),
                    partitions=["symbol", "datetime"],
                ),
                mode=EnumIngestionMode.APPEND,
            ),
        )

    def run(self):
        print(f"Data recebida: {self.date}")
        stocks = self.create_dataframe()
        self.save(stocks)
