import os
import magic
import chardet
import zipfile
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

class FileTypeResult(BaseModel):
    format: str
    encoding: str
    delimiter: Optional[str] = None
    sheet_names: Optional[List[str]] = None
    is_compressed: bool
    mime_type: str
    contained_files: Optional[List[Dict[str, Any]]] = None

def get_mime_type(sample_bytes: bytes) -> str:
    """Never trust file extension alone."""
    mime = magic.Magic(mime=True)
    return mime.from_buffer(sample_bytes)

def detect_file_type(file_path: str) -> FileTypeResult:
    """
    Main detection logic utilizing magic and chardet on the first 100KB.
    """
    # 1. Read sample
    sample_size = 100 * 1024
    with open(file_path, "rb") as f:
        sample_bytes = f.read(sample_size)
    
    mime_type = get_mime_type(sample_bytes)
    
    # 2. Detect Encoding
    char_det_res = chardet.detect(sample_bytes)
    encoding = char_det_res["encoding"] or "utf-8"
    
    # 3. Establish defaults
    is_compressed = False
    delimiter = None
    sheet_names = None
    format_type = "unknown"
    contained_files = None
    
    # 4. MIME specific inference
    if "zip" in mime_type:
        is_compressed = True
        format_type = "zip"
        contained_files = []
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                for info in zf.infolist():
                    contained_files.append({"filename": info.filename, "size": info.file_size})
        except zipfile.BadZipFile:
            pass

    elif "spreadsheet" in mime_type or "excel" in mime_type or (len(sample_bytes) > 4 and sample_bytes[:4] == b"PK\x03\x04"):
        format_type = "excel"
        import openpyxl
        try:
            wb = openpyxl.load_workbook(file_path, read_only=True, keep_links=False)
            sheet_names = wb.sheetnames
        except Exception:
            pass

    elif "parquet" in mime_type or "octet-stream" in mime_type:
        if sample_bytes.startswith(b"PAR1"):
            format_type = "parquet"

    elif "json" in mime_type:
        format_type = "json"

    elif "text" in mime_type or "csv" in mime_type:
        try:
            sample_text = sample_bytes.decode(encoding)
            # Sniff delimiter
            if "\t" in sample_text and sample_text.count("\t") > sample_text.count(","):
                delimiter = "\t"
                format_type = "tsv"
            else:
                delimiter = ","
                format_type = "csv"
        except UnicodeDecodeError:
            format_type = "csv" # Fallback
            delimiter = ","
            
    return FileTypeResult(
        format=format_type,
        encoding=encoding,
        delimiter=delimiter,
        sheet_names=sheet_names,
        is_compressed=is_compressed,
        mime_type=mime_type,
        contained_files=contained_files
    )
