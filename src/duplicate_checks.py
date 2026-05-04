import pandas as pd


DEFAULT_DUPLICATE_RATE_THRESHOLD = 0.0


def calculate_exact_duplicate_count(df: pd.DataFrame) -> int:
    return int(df.duplicated().sum())


def calculate_exact_duplicate_rate(df: pd.DataFrame) -> float:
    if len(df) == 0:
        return 0.0

    duplicate_count = calculate_exact_duplicate_count(df)
    return round(duplicate_count / len(df), 4)


def get_exact_duplicate_rows(df: pd.DataFrame, max_rows: int = 10) -> list:
    duplicate_rows = df[df.duplicated(keep=False)]
    return duplicate_rows.head(max_rows).to_dict(orient="records")


def check_exact_duplicates(
    df: pd.DataFrame,
    duplicate_rate_threshold: float = DEFAULT_DUPLICATE_RATE_THRESHOLD
) -> dict:

    duplicate_count = calculate_exact_duplicate_count(df)
    duplicate_rate = calculate_exact_duplicate_rate(df)

    status = "PASS" if duplicate_rate <= duplicate_rate_threshold else "FAIL"

    return {
        "check": "exact_duplicates",
        "status": status,
        "duplicate_count": duplicate_count,
        "duplicate_rate": duplicate_rate,
        "threshold": duplicate_rate_threshold,
        "sample_duplicate_rows": get_exact_duplicate_rows(df)
    }


def check_duplicate_columns(df: pd.DataFrame, unique_columns: list = None) -> dict:

    if unique_columns is None:
        unique_columns = []

    failed_columns = {}
    skipped_columns = []

    for column in unique_columns:
        if column not in df.columns:
            skipped_columns.append(column)
            continue

        duplicate_mask = df[column].duplicated(keep=False)
        duplicate_values = df.loc[duplicate_mask, column]

        duplicate_count = int(duplicate_mask.sum())
        unique_duplicate_values = sorted(
            duplicate_values.dropna().unique().tolist(),
            key=lambda value: str(value)
        )

        if duplicate_count > 0:
            failed_columns[column] = {
                "duplicate_count": duplicate_count,
                "duplicate_values": unique_duplicate_values[:20]
            }

    if len(failed_columns) > 0:
        status = "FAIL"
    elif len(skipped_columns) > 0:
        status = "WARNING"
    else:
        status = "PASS"

    return {
        "check": "duplicate_unique_columns",
        "status": status,
        "unique_columns_checked": unique_columns,
        "failed_columns": failed_columns,
        "skipped_columns": skipped_columns
    }


def check_duplicate_subset(
    df: pd.DataFrame,
    subset_columns: list = None,
    duplicate_rate_threshold: float = DEFAULT_DUPLICATE_RATE_THRESHOLD
) -> dict:

    if subset_columns is None or len(subset_columns) == 0:
        return {
            "check": "duplicate_subset",
            "status": "SKIPPED",
            "reason": "No subset columns provided.",
            "subset_columns": []
        }

    missing_subset_columns = [column for column in subset_columns if column not in df.columns]

    if len(missing_subset_columns) > 0:
        return {
            "check": "duplicate_subset",
            "status": "WARNING",
            "reason": "Some subset columns are missing from the dataframe.",
            "subset_columns": subset_columns,
            "missing_subset_columns": missing_subset_columns
        }

    if len(df) == 0:
        duplicate_count = 0
        duplicate_rate = 0.0
    else:
        duplicate_mask = df.duplicated(subset=subset_columns)
        duplicate_count = int(duplicate_mask.sum())
        duplicate_rate = round(duplicate_count / len(df), 4)

    status = "PASS" if duplicate_rate <= duplicate_rate_threshold else "FAIL"

    sample_duplicate_rows = (
        df[df.duplicated(subset=subset_columns, keep=False)]
        .head(10)
        .to_dict(orient="records")
    )

    return {
        "check": "duplicate_subset",
        "status": status,
        "subset_columns": subset_columns,
        "duplicate_count": duplicate_count,
        "duplicate_rate": duplicate_rate,
        "threshold": duplicate_rate_threshold,
        "sample_duplicate_rows": sample_duplicate_rows
    }


def run_duplicate_checks(
    df: pd.DataFrame,
    unique_columns: list = None,
    subset_columns: list = None,
    duplicate_rate_threshold: float = DEFAULT_DUPLICATE_RATE_THRESHOLD
) -> dict:

    exact_result = check_exact_duplicates(
        df=df,
        duplicate_rate_threshold=duplicate_rate_threshold
    )

    unique_column_result = check_duplicate_columns(
        df=df,
        unique_columns=unique_columns
    )

    subset_result = check_duplicate_subset(
        df=df,
        subset_columns=subset_columns,
        duplicate_rate_threshold=duplicate_rate_threshold
    )

    statuses = [
        exact_result["status"],
        unique_column_result["status"],
        subset_result["status"]
    ]

    if "FAIL" in statuses:
        overall_status = "FAIL"
    elif "WARNING" in statuses:
        overall_status = "WARNING"
    else:
        overall_status = "PASS"

    return {
        "check": "duplicate_checks",
        "status": overall_status,
        "details": {
            "exact_duplicates": exact_result,
            "unique_column_duplicates": unique_column_result,
            "subset_duplicates": subset_result
        }
    }