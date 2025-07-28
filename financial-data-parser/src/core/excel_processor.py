from __future__ import annotations
import os
from typing import Dict, List, Any, Optional
import pandas as pd
from typing import List, Dict, Tuple

class ExcelProcessor:
    def __init__(self) -> None:
        self._workbooks: Dict[str, pd.ExcelFile] = {}
        self.full_data: Dict[str, Dict[str, pd.DataFrame]] = {}

    def load_files(self, file_paths: List[str]) -> None:
        for path in file_paths:
            if not os.path.isfile(path):
                raise FileNotFoundError(path)
            xls = pd.ExcelFile(path)
            self._workbooks[path] = xls
            self.full_data[path] = {}
            for sheet in xls.sheet_names:
                self.full_data[path][sheet] = xls.parse(sheet_name=sheet)

    def get_sheet_info(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        info: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for path, sheets in self.full_data.items():
            info[path] = {}
            for sheet_name, df in sheets.items():
                info[path][sheet_name] = {
                    "rows": len(df),
                    "cols": len(df.columns),
                    "columns": list(df.columns),
                    "dtypes": {col: str(df[col].dtype) for col in df.columns}
                }
        return info

    def preview_data(
        self,
        file_path: str,
        sheet_name: str,
        rows: int = 5,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if file_path not in self.full_data:
            raise ValueError(f"File not loaded: {file_path}")
        if sheet_name not in self.full_data[file_path]:
            raise ValueError(f"Sheet not found: {sheet_name}")
        df = self.full_data[file_path][sheet_name]
        return df.head(rows) if columns is None else df[columns].head(rows)

    def get_full_data(self, file_path: str, sheet_name: str) -> pd.DataFrame:
        if file_path not in self.full_data:
            raise ValueError(f"File not loaded: {file_path}")
        if sheet_name not in self.full_data[file_path]:
            raise ValueError(f"Sheet not found: {sheet_name}")
        return self.full_data[file_path][sheet_name].copy()