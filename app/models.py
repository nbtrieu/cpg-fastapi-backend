from pydantic import BaseModel
from typing import List


class FactorRequest(BaseModel):
    factors: List[str]
    cpg_group_name: str
