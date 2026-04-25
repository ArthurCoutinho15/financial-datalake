from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from .iceberg_tables import IcebergTableConfig


class EnumMergeStrategy(str, Enum):
    TYPE1: str = "type1"
    TYPE2: str = "type2"


class EnumIngestionMode(str, Enum):
    UPSERT: str = "UPSERT"
    APPEND: str = "APPEND"


@dataclass
class WriterConfig:
    iceberg_table_cfg: IcebergTableConfig

    mode: EnumIngestionMode = EnumIngestionMode.APPEND
    strategy: Optional[EnumMergeStrategy] = None

    # SCD Type 1
    merge_columns: Optional[List[str]] = None

    # SCD Type 2
    business_keys: Optional[List[str]] = None
    compare_columns: Optional[List[str]] = None

    def __post_init__(self):
        if self.mode == EnumIngestionMode.UPSERT:
            if self.strategy == EnumMergeStrategy.TYPE1 and not self.merge_columns:
                raise ValueError("merge_columns are mandatory for TYPE1")

            if self.strategy == EnumMergeStrategy.TYPE2 and (
                not self.business_keys or not self.compare_columns
            ):
                raise ValueError(
                    "business_keys and compare_columns are mandatory for TYPE2"
                )
