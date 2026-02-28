from abc import ABC, abstractmethod
from typing import Dict, Any, List
import pandas as pd

class PipelineContext:
    def __init__(self, dataset_id: str, domain: str, job_id: str, options: dict = None):
        self.dataset_id = dataset_id
        self.domain = domain
        self.job_id = job_id
        self.options = options or {}
        self.schema = {}
        self.is_dask = False
        self.warnings: List[str] = []

class StepResult:
    def __init__(self, df: Any, logs: List[Dict[str, Any]], warnings: List[str], modified_columns: List[str]):
        self.df = df
        self.logs = logs
        self.warnings = warnings
        self.modified_columns = modified_columns

class PipelineStep(ABC):
    @abstractmethod
    def run(self, df: Any, context: PipelineContext) -> StepResult:
        pass
