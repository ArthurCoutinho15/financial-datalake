from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DeequCheckConfig:
    column: str
    check_type: str  # completeness, uniqueness, min, max, etc.
    threshold: Optional[float] = None
    value: Optional[float] = None


@dataclass
class DeequConfig:
    checks: List[DeequCheckConfig]
