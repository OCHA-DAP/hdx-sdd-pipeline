import pandas as pd


def concatenate_header(df: pd.DataFrame, numeric_threshold: float = 0.8) -> pd.DataFrame:
    """
    Combined header concatenation:
    1. First attempts to find the first row without NaN (fully populated row).
    2. If none found, falls back to detecting numeric columns.
    3. Fills missing header cells horizontally.
    4. Concatenates the header rows into single column names.
    """

    # ---- STEP 1: try first fully populated row ---- #
    header_end_row = None
    for idx, row in df.iterrows():
        if row.notna().all():
            header_end_row = idx
            break

    # ---- STEP 2: fallback to numeric detection if no full row found ---- #
    if header_end_row is None:
        numeric_ratio = df.apply(lambda col: pd.to_numeric(col, errors='coerce').notna().mean())
        numeric_cols = numeric_ratio[numeric_ratio >= numeric_threshold].index.tolist()

        if numeric_cols:
            for idx, row in df.iterrows():
                if row[numeric_cols].apply(lambda x: pd.to_numeric(x, errors='coerce')).notna().any():
                    header_end_row = idx
                    break
        else:
            # no numeric columns detected, return original df
            return df

    # ---- STEP 3: extract header block ---- #
    header_block = df.iloc[: header_end_row + 1].copy()
    header_block = header_block.fillna("").astype(str)

    # ---- STEP 4: fill horizontally missing cells ---- #
    header_block = header_block.apply(lambda row: row.replace("", None).ffill(), axis=1)
    header_block = header_block.replace("", None).ffill()  # vertical fill

    # ---- STEP 5: concatenate header rows ---- #
    final_columns = header_block.apply(lambda col: " | ".join([v for v in col if v]), axis=0)

    # ---- STEP 6: assign as header ---- #
    cleaned_df = df.iloc[header_end_row + 1 :].copy()
    cleaned_df.columns = final_columns

    return cleaned_df.reset_index(drop=True)


# Example usage
if __name__ == '__main__':
    FILE_PATH = 'test/unit/downloads/multicolumn_sample.xlsx'
    df = pd.read_excel(FILE_PATH, header=None)
    final_df = concatenate_header(df)
    print(final_df.head())
    print(final_df.columns)

    FILE_PATH = (
        '/Users/liangtelkamp/Documents/GitHub/hdx-ssd-pipeline/test/unit/downloads/Country Profiles Oct 14 2025.xlsx'
    )
    df = pd.read_excel(FILE_PATH, header=None)
    final_df = concatenate_header(df)
    print(final_df.head())
    print(final_df.columns)
