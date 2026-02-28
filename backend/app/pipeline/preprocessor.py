import traceback
import logging
from typing import Any, List
from .steps.base import PipelineContext, PipelineStep
from .steps.normalizer import ColumnNormalizer
from .steps.schema_analyzer import SchemaAnalyzer
from .steps.type_fixer import TypeFixer
from .steps.missing_handler import MissingValueHandler
from .steps.duplicate_detector import DuplicateDetector
from .steps.outlier_detector import OutlierDetector
from .steps.encoder_suggester import EncoderSuggester

logger = logging.getLogger(__name__)

class PreprocessingOrchestrator:
    def __init__(self):
        self.steps: List[PipelineStep] = [
            ColumnNormalizer(),
            SchemaAnalyzer(),
            TypeFixer(),
            MissingValueHandler(),
            DuplicateDetector(),
            OutlierDetector(),
            EncoderSuggester()
        ]
        
        # Progress mappings for steps
        self.progress_mapping = {
            "ColumnNormalizer": 10.0,
            "SchemaAnalyzer": 20.0,
            "TypeFixer": 35.0,
            "MissingValueHandler": 50.0,
            "DuplicateDetector": 65.0,
            "OutlierDetector": 80.0,
            "EncoderSuggester": 90.0
        }

    def run_pipeline(self, df: Any, context: PipelineContext) -> Any:
        current_df = df
        all_logs = []
        
        for step in self.steps:
            step_name = step.__class__.__name__
            logger.info(f"Running pipeline step: {step_name} for dataset {context.dataset_id}")
            try:
                result = step.run(current_df, context)
                current_df = result.df
                all_logs.extend(result.logs)
                context.warnings.extend(result.warnings)
                
                # TODO: Redis Progress Update via context.job_id 
                # e.g., update_progress(context.job_id, self.progress_mapping[step_name], step_name)
                
            except Exception as e:
                logger.error(f"Step {step_name} failed: {traceback.format_exc()}")
                # Mark job as failed gracefully
                raise RuntimeError(f"Pipeline step {step_name} failed: {str(e)}")
        
        # At the end, if Dask, DO NOT compute, it's computed at the save step 
        logger.info(f"Pipeline completed for dataset {context.dataset_id}")
        return current_df, all_logs
