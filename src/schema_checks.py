import pandas as pd


def extract_schema(df: pd.DataFrame) -> dict:
    return {col: str(dtype) for col, dtype in df.dtypes.items()}


# In case no predefined schema was provided
def infer_schema_from_baseline(baseline_df: pd.DataFrame) -> dict:
    return extract_schema(baseline_df)


def check_missing_columns(df: pd.DataFrame, schema: dict) -> dict:
    columns = set(df.columns)
    expected_columns = set(schema.keys())

    missing = list(expected_columns - columns)

    return {"check": "missing_columns",
            "status": "PASS" if len(missing) == 0 else "FAIL",
            "missing_columns": missing}


def check_extra_columns(df: pd.DataFrame, schema: dict) -> dict:
    columns = set(df.columns)
    expected_columns = set(schema.keys())

    extra = list(columns - expected_columns)

    return {"check": "extra_columns",
            "status": "PASS" if len(extra) == 0 else "WARNING",
            "missing_columns": extra}


def check_column_dtypes(df: pd.DataFrame, expected_schema: dict) -> dict:
    actual_schema = extract_schema(df)
    dtype_mismatches = {}

    for column, expected_dtype in expected_schema.items():
        if column in actual_schema:
            actual_dtype = actual_schema[column]
            if actual_dtype != expected_dtype:
                dtype_mismatches[column] = {"expected": expected_dtype, "actual": actual_dtype}

    return {"check": "dtype_check",
            "status": "PASS" if len(dtype_mismatches) == 0 else "FAIL",
            "dtype_mismatches": dtype_mismatches}


def run_schema_checks(df: pd.DataFrame, expected_schema: dict = None, baseline_df: pd.DataFrame = None) -> dict:
    if expected_schema is None:
        if baseline_df is None:
            raise ValueError("You must provide either expected_schema or baseline_df.")
        expected_schema = infer_schema_from_baseline(baseline_df)

    missing_result = check_missing_columns(df, expected_schema)
    extra_result = check_extra_columns(df, expected_schema)
    dtype_result = check_column_dtypes(df, expected_schema)

    overall_status = "PASS"

    if missing_result["status"] == "FAIL" or dtype_result["status"] == "FAIL":
        overall_status = "FAIL"
    elif extra_result["status"] == "WARNING":
        overall_status = "WARNING"

    return {
        "check": "schema_checks",
        "status": overall_status,
        "expected_schema": expected_schema,
        "details": {
            "missing_columns": missing_result,
            "extra_columns": extra_result,
            "dtype_check": dtype_result
        }
    }
