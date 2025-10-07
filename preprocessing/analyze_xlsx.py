"""
analyze_xlsx.py

analyze_excel_file(file_path) ->
{
  "metadatasheet": bool,           # True if any sheet looks like metadata/explanations
  "multiple_sheets": bool,         # True if workbook has more than one sheet
  "multiple_tables": {             # per-sheet bool whether that sheet contains multiple tables
      "<sheet name>": True/False,
      ...
  }
}

Improved multiple-table detection uses connected components on the non-empty-cell mask.
"""

from typing import Dict, List, Tuple
import pandas as pd
import math

def _is_number(val) -> bool:
    try:
        # treat booleans as non-numeric for our purposes
        if isinstance(val, bool):
            return False
        float(val)
        return True
    except Exception:
        return False

def _connected_components(mask: pd.DataFrame) -> List[List[Tuple[int,int]]]:
    R, C = mask.shape
    visited = [[False]*C for _ in range(R)]
    components = []
    for i in range(R):
        for j in range(C):
            if not mask.iat[i, j] or visited[i][j]:
                continue
            # BFS/DFS to collect connected True-cells (4-neighbour)
            stack = [(i, j)]
            comp = []
            visited[i][j] = True
            while stack:
                x, y = stack.pop()
                comp.append((x, y))
                for dx, dy in ((1,0),(-1,0),(0,1),(0,-1)):
                    nx, ny = x+dx, y+dy
                    if 0 <= nx < R and 0 <= ny < C and not visited[nx][ny] and mask.iat[nx, ny]:
                        visited[nx][ny] = True
                        stack.append((nx, ny))
            components.append(comp)
    return components

def _component_stats(comp: List[Tuple[int,int]]) -> Dict:
    rows = [r for r,_ in comp]
    cols = [c for _,c in comp]
    min_r, max_r = min(rows), max(rows)
    min_c, max_c = min(cols), max(cols)
    bbox_rows = max_r - min_r + 1
    bbox_cols = max_c - min_c + 1
    num_cells = len(comp)
    density = num_cells / (bbox_rows * bbox_cols) if bbox_rows * bbox_cols > 0 else 0
    return {
        "num_cells": num_cells,
        "min_r": min_r, "max_r": max_r,
        "min_c": min_c, "max_c": max_c,
        "bbox_rows": bbox_rows, "bbox_cols": bbox_cols,
        "density": density
    }

def analyze_excel_file(file_path: str, min_table_cells: int = 3) -> Dict:
    """
    Analyze an Excel file and return:
    {
      "metadatasheet": bool,
      "multiple_sheets": bool,
      "multiple_tables": { sheetname: bool, ... }
    }

    Parameters:
      - file_path: path to .xlsx
      - min_table_cells: smallest connected non-empty cell count to consider (default 3)
    """
    xls = pd.ExcelFile(file_path)
    sheet_names = xls.sheet_names

    results = {
        "metadatasheet": False,
        "multiple_sheets": len(sheet_names) > 1,
        "multiple_tables": {}
    }

    metadata_keywords = {"description", "about", "info", "metadata", "explanation", "notes", "readme"}

    metadata_sheets = set()

    for sheet in sheet_names:
        # read sheet with no header and as objects so we can inspect types
        df = pd.read_excel(file_path, sheet_name=sheet, header=None, dtype=object)

        # strip outer empty rows/cols to focus on used area
        df = df.dropna(how="all")
        df = df.dropna(axis=1, how="all")
        if df.shape[0] == 0 or df.shape[1] == 0:
            # empty sheet => no tables
            results["multiple_tables"][sheet] = False
            continue

        # create non-empty mask: treat empty strings / whitespace as empty
        non_empty = df.applymap(lambda x: not (pd.isna(x) or (isinstance(x, str) and x.strip() == "")))

        total_cells = df.size
        non_empty_count = non_empty.values.sum()
        # compute text vs numeric approx
        text_count = 0
        numeric_count = 0
        for r, c in zip(*non_empty.to_numpy().nonzero()):
            val = df.iat[r, c]
            if _is_number(val):
                numeric_count += 1
            else:
                text_count += 1
        text_ratio = text_count / non_empty_count if non_empty_count else 0
        numeric_ratio = numeric_count / non_empty_count if non_empty_count else 0

        # --- metadata detection (per-sheet) ---
        sheet_name_lower = sheet.lower()
        blob_text = " ".join(map(str, df.astype(str).fillna("").values.flatten())).lower()
        keyword_in_name_or_blob = any(kw in sheet_name_lower or kw in blob_text for kw in metadata_keywords)

        is_metadata = False
        # Heuristic: name contains keyword OR very high text ratio and very low numeric ratio
        if keyword_in_name_or_blob or (text_ratio > 0.8 and numeric_ratio < 0.15):
            is_metadata = True
            metadata_sheets.add(sheet)
            results["metadatasheet"] = True

        # If metadata, don't mark as multiple tables (typical expectation)
        if is_metadata:
            results["multiple_tables"][sheet] = False
            continue

        # --- detect connected components of non-empty cells ---
        components = _connected_components(non_empty)

        # evaluate each component whether it looks like a table
        table_components = 0
        for comp in components:
            stats = _component_stats(comp)
            nc = stats["num_cells"]
            br = stats["bbox_rows"]
            bc = stats["bbox_cols"]
            density = stats["density"]

            # ignore tiny stray components (notes, single-cell annotations)
            if nc < min_table_cells:
                continue

            # compute numeric ratio inside component
            numeric_in_comp = 0
            for (r,c) in comp:
                if _is_number(df.iat[r,c]):
                    numeric_in_comp += 1
            comp_numeric_ratio = numeric_in_comp / nc if nc else 0

            # Heuristics to decide if a component is a "table"
            is_table = False

            # 1) rectangular with at least 2 rows and 2 cols and a modest density
            if br >= 2 and bc >= 2 and nc >= max(4, 0.25 * br * bc):
                is_table = True

            # 2) single-column tall region that is mostly numeric (like a column of values)
            #    treat text-only single-column regions as NOT tables (likely metadata/notes)
            elif br >= 3 and bc == 1 and comp_numeric_ratio > 0.25 and nc >= 3:
                is_table = True

            # 3) single-row wide region (e.g., header-only) is not a table by itself,
            #    unless it is wide and has more than 1 row visually (we skip)
            # 4) additional rule: density high (e.g., >0.6) and either multiple rows or multiple cols
            elif (br >= 2 or bc >= 2) and density >= 0.6 and nc >= 4:
                is_table = True

            if is_table:
                table_components += 1

        results["multiple_tables"][sheet] = (table_components > 1)

    return results


# Quick example / CLI usage
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python analyze_xlsx.py <file.xlsx>")
        sys.exit(1)
    path = sys.argv[1]
    res = analyze_excel_file(path)
    import json
    print(json.dumps(res, indent=2))
