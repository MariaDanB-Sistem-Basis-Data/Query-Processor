from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, List, TypeVar, Union

from .Rows import Rows

@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime
    message: str
    data: Union[Rows, int] # data can be either rows  or an int
    query: str
