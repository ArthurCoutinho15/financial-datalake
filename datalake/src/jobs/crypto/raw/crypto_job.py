from datetime import date

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from clients.spark_client import spark_client
from clients.crypto_stocks_client import CryptoStocksClient
from .crypto_table import RawCryptoTable


class RawCryptoJob:
    def __init__(self, date: date):
        self.date = date
        self.table = RawCryptoTable()
        self.client = CryptoStocksClient()
        self.spark: SparkSession = spark_client.get_session()
        self.symbols = [
            "BTC/USD",
            "ETH/USD",
            "SOL/USD",
            "ADA/USD",
            "DOGE/USD",
        ]

    def _get_data(self) -> list[str]:
        return self.client.get_stocks(date=self.date, stocks_symbol=self.symbols)

    def clean_data(self) -> list[dict]:
        data = self._get_data()

        records = []

        for symbol, crypto_data in data.items():
            values = crypto_data.get("values", [])

            for row in values:
                records.append(
                    {
                        "symbol": symbol,
                        "datetime": row.get("datetime"),
                        "open": row.get("open"),
                        "high": row.get("high"),
                        "low": row.get("low"),
                        "close": row.get("close"),
                    }
                )

        return records

    def create_dataframe(self, data: list[str]) -> DataFrame:
        return self.spark.createDataFrame(data, schema=self.table.schema())

    def create_dt_reference_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("dt_reference", F.lit(self.date))

    def save(self, df: DataFrame) -> None:
        spark_client.create_iceberg_table(
            table_name=self.table.full_name(),
            schema=self.table.schema(),
            partitions=["symbol", "datetime"],
        )
        
        spark_client.merge_data(
            table_name=self.table.full_name(),
            df=df,
            merge_columns=["symbol", "datetime"]
        )

    def run(self) -> None:
        crypto_data = self.clean_data()
        crypto_df = self.create_dataframe(data=crypto_data)
        crypto_df = self.create_dt_reference_column(crypto_df)
        self.save(crypto_df)

        crypto_df.show()
