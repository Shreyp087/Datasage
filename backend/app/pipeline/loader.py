import os
import pandas as pd
import dask.dataframe as dd
import tempfile
import zipfile
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class DatasetLoadError(Exception):
    pass
class ValidationError(Exception):
    pass

class DataFrameResult:
    def __init__(self, df: Any, row_count: int, col_count: int, memory_usage_mb: float, detected_dtypes: dict, is_dask: bool, warnings: List[str]):
        self.df = df
        self.row_count = row_count
        self.col_count = col_count
        self.memory_usage_mb = memory_usage_mb
        self.detected_dtypes = detected_dtypes
        self.is_dask = is_dask
        self.warnings = warnings

class DatasetLoader:
    def __init__(self):
        self.warnings = []

    def load(self, file_path: str, file_type: Dict[str, Any], options: Dict[str, Any]) -> DataFrameResult:
        """
        Loads the dataset smartly based on file size and sniffed dtypes.
        Files < 500MB: memory pandas
        Files 500MB - 2GB: optimized pandas
        Files > 2GB: Dask DataFrame
        """
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        is_dask = False
        
        # 1. ZIP handling
        if file_type.get('format') == 'zip':
            temp_dir = tempfile.mkdtemp()
            with zipfile.ZipFile(file_path, 'r') as zf:
                zf.extractall(temp_dir)
                extracted_files = zf.namelist()
                if not extracted_files:
                    raise ValidationError("ZIP file is empty")
                # Load first file for simplicity in this scaffold
                file_path = os.path.join(temp_dir, extracted_files[0])
                # We should re-detect but we assume CSV for scaffold
                file_type['format'] = 'csv'
                file_type['delimiter'] = ','

        # 2. Sniff Types
        try:
            dtype_map = self.sniff_dtypes(file_path, file_type)
        except Exception as e:
            self.warnings.append(f"Failed to sniff dtypes: {str(e)}")
            dtype_map = None

        # 3. Load Engine selection
        df = None
        encoding = file_type.get('encoding', 'utf-8')
        
        kwargs = {
            "dtype": dtype_map,
            "encoding": encoding
        }
        
        try:
            if file_size_mb > 2000:
                # > 2GB: Dask
                is_dask = True
                logger.info("Using Dask DataLoader (Large file > 2GB)")
                if file_type['format'] == 'csv':
                    df = dd.read_csv(file_path, sep=file_type.get('delimiter', ','), **kwargs)
                elif file_type['format'] == 'parquet':
                    df = dd.read_parquet(file_path)
            else:
                # Pandas
                logger.info(f"Using Pandas DataLoader (File {file_size_mb:.2f} MB)")
                extra_args = {"low_memory": False} if file_size_mb > 500 else {}
                
                if file_type['format'] == 'csv':
                    df = pd.read_csv(file_path, sep=file_type.get('delimiter', ','), **kwargs, **extra_args)
                elif file_type['format'] == 'excel':
                    df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl')
                elif file_type['format'] == 'parquet':
                    df = pd.read_parquet(file_path)
                elif file_type['format'] == 'json':
                    # Try lines=True first
                    try:
                        df = pd.read_json(file_path, lines=True)
                    except ValueError:
                        df = pd.read_json(file_path)
                        
        except UnicodeDecodeError:
            self.warnings.append(f"Encoding '{encoding}' failed. Retrying with 'latin-1'")
            df = pd.read_csv(file_path, sep=file_type.get('delimiter', ','), encoding='latin-1')
        except MemoryError:
            self.warnings.append("Memory error encountered during Pandas load. Falling back to Dask.")
            is_dask = True
            df = dd.read_csv(file_path, sep=file_type.get('delimiter', ','))
        except Exception as e:
            raise DatasetLoadError(f"Corrupted file or load failure: {str(e)}")
            
        if df is None or len(df.columns) == 0:
            raise ValidationError("Dataset is empty or format unrecognized.")

        # Compute stats (cheaply for Dask)
        col_count = len(df.columns)
        
        if is_dask:
            # Avoid full compute
            row_count = len(df) # len() works cheaply on dask index typically, or requires compute
            memory_usage_mb = 0.0 # difficult to estimate without compute
        else:
            row_count = len(df)
            memory_usage_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

        if row_count == 0:
            raise ValidationError("File contains no rows.")

        return DataFrameResult(df, row_count, col_count, memory_usage_mb, dtype_map or {}, is_dask, self.warnings)

    def sniff_dtypes(self, file_path: str, file_type: dict, sample_rows: int = 10000) -> dict:
        """
        Reads first N rows to aggressively downcast numeric types and categorize objects.
        """
        if file_type.get('format') not in ['csv', 'tsv']:
            return {}

        sample = pd.read_csv(file_path, sep=file_type.get('delimiter', ','), nrows=sample_rows, encoding=file_type.get('encoding', 'utf-8'))
        
        optimized_dtypes = {}
        for col in sample.columns:
            dt = sample[col].dtype
            if pd.api.types.is_numeric_dtype(dt):
                d_min, d_max = sample[col].min(), sample[col].max()
                if pd.api.types.is_integer_dtype(dt):
                    if d_min >= -32768 and d_max <= 32767:
                        optimized_dtypes[col] = 'Int16' # Nullable int
                    elif d_min >= -2147483648 and d_max <= 2147483647:
                        optimized_dtypes[col] = 'Int32'
                    else:
                        optimized_dtypes[col] = 'Int64'
            elif pd.api.types.is_object_dtype(dt):
                nunique = sample[col].nunique()
                if nunique < 50 and nunique < (len(sample) / 2):
                    optimized_dtypes[col] = 'category'
                    
        return optimized_dtypes
