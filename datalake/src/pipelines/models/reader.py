from enum import Enum
from dataclasses import dataclass


class EnumReadMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    INCREMENTAL_WITH_HISTORY = "incremental_with_history"


@dataclass
class ReaderConfig:
    source_table: str
    target_table: str
    date_column: str
    mode: EnumReadMode
