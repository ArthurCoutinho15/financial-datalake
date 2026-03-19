from datetime import date
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from .raw_table import RawCoinsTable
from clients.banco_central_api_client import BancoCentralApiClient
from clients.spark_client import spark_client


class RawCoins:
    def __init__(self, date: date = date.today()) -> None:
        self.table = RawCoinsTable()
        self.client = BancoCentralApiClient(
            base_url="https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/",
            timeout=30,
        )
        self.spark: SparkSession = spark_client.get_session()
        self.date = date

    def _get_coins_symbols(self) -> list[str]:
        json_data = self.client.get_moedas()
        coin_data = json_data["value"]

        symbols = []

        for symbol in coin_data:
            symbols.append(symbol["simbolo"])

        return symbols

    def get_daily_cotation(self) -> list[dict]:
        symbols = self._get_coins_symbols()

        records: list = []

        for symbol in symbols:
            data = self.client.get_coin_daily_cotation(
                moeda=symbol, cotation_date=self.date
            )

            if data["value"]:
                record = data["value"][0]
                record["symbol"] = symbol

                records.append(record)

        print(records)
        return records

    def create_dataframe(self) -> DataFrame:
        coins_daily_cotation = self.get_daily_cotation()

        df: DataFrame = self.spark.createDataFrame(
            coins_daily_cotation, schema=self.table.schema()
        )

        df = df.withColumn("dt_reference", F.lit(self.date))

        return df

    def save(self, df: DataFrame) -> None:
        spark_client.create_iceberg_table(
            table_name=self.table.full_name(),
            schema=self.table.schema(),
            partitions=["dt_reference"],
        )
        df.writeTo(self.table.full_name()).overwritePartitions()

    def run(self):
        df = self.create_dataframe()
        self.save(df)

        df.show()
