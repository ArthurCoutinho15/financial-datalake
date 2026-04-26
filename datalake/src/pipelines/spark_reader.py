from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from requests import ReadTimeout

from .spark_client import spark_client
from pipelines.models.reader import EnumReadMode, ReaderConfig


class SparkReader:
    def __init__(self):
        self._spark: SparkSession = spark_client.get_session()

    def _read_table(self, table_name: str) -> DataFrame:
        return self._spark.read.table(table_name)

    def _get_last_processed_data(self, table_name: str, dt_column: str) -> date:
        df = self._read_table(table_name)

        last_date = df.agg(F.max(F.col(dt_column))).collect()[0][0]
        
        return last_date

    def get_data(self, cfg: ReaderConfig) -> DataFrame:

        source_df = self._read_table(table_name=cfg.source_table)

        try:
            last_date = self._get_last_processed_data(
                table_name=cfg.target_table, dt_column=cfg.date_column
            )
        except:
            return source_df

        if not last_date:
            return source_df

        if cfg.mode == EnumReadMode.INCREMENTAL:
            return source_df.filter(F.col(cfg.date_column) > F.lit(last_date))
        elif cfg.mode == EnumReadMode.INCREMENTAL_WITH_HISTORY:
            if last_date:
                return source_df.filter(
                    F.col(cfg.date_column) >= F.date_sub(F.lit(last_date), 1)
                )
        elif cfg.mode == EnumReadMode.FULL:
            return source_df
        else:
            raise ValueError("Read mode is mandatory.")
