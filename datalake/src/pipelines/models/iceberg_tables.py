from dataclasses import dataclass
from typing import List

import pyspark.sql.types as t


@dataclass
class IcebergTableConfig:
    table_name: str
    schema: t.StructType
    partitions: List[str]
