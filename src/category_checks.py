import pandas as pd


DEFAULT_INVALID_CATEGORY_RATE_THRESHOLD = 0.0


def get_categorical_columns(df: pd.DataFrame, include_numeric_low_cardinality: bool = False, max_unique_values: int = 20
) -> list:
    categorical_columns = []

    for column in df.columns:
        series = df[column]

        if (pd.api.types.is_object_dtype(series) or pd.api.types.CategoricalDtype(series)
                or pd.api.types.is_bool_dtype(series)):
            categorical_columns.append(column)

        elif include_numeric_low_cardinality and pd.api.types.is_numeric_dtype(series):
            unique_count = series.dropna().nunique()
            if unique_count <= max_unique_values:
                categorical_columns.append(column)

    return categorical_columns


def infer_allowed_categories_from_baseline(
        baseline_df: pd.DataFrame,
        categorical_columns: list = None,
        include_numeric_low_cardinality: bool = False,
        max_unique_values: int = 20) -> dict:
    if categorical_columns is None:
        categorical_columns = get_categorical_columns(
            baseline_df,
            include_numeric_low_cardinality=include_numeric_low_cardinality,
            max_unique_values=max_unique_values
        )

    allowed_categories = {}

    for column in categorical_columns:
        if column in baseline_df.columns:
            values = baseline_df[column].dropna().unique().tolist()
            allowed_categories[column] = sorted(values, key=lambda value: str(value))

    return allowed_categories


def calculate_invalid_category_details(
    df: pd.DataFrame,
    allowed_categories: dict,
    invalid_rate_threshold: float = DEFAULT_INVALID_CATEGORY_RATE_THRESHOLD
) -> dict:
    failed_columns = {}
    warning_columns = {}
    passed_columns = {}
    skipped_columns = []

    for column, allowed_values in allowed_categories.items():
        if column not in df.columns:
            skipped_columns.append(column)
            continue

        allowed_set = set(allowed_values)

        non_missing_series = df[column].dropna()
        total_non_missing = len(non_missing_series)

        if total_non_missing == 0:
            passed_columns[column] = {
                "invalid_count": 0,
                "invalid_rate": 0.0,
                "invalid_values": [],
                "allowed_values": allowed_values
            }
            continue

        invalid_mask = ~non_missing_series.isin(allowed_set)
        invalid_values_series = non_missing_series[invalid_mask]

        invalid_count = int(invalid_values_series.shape[0])
        invalid_rate = round(invalid_count / total_non_missing, 4)

        invalid_value_counts = {
            str(value): int(count)
            for value, count in invalid_values_series.value_counts().items()
        }

        column_result = {
            "invalid_count": invalid_count,
            "invalid_rate": invalid_rate,
            "invalid_values": sorted(list(invalid_value_counts.keys())),
            "invalid_value_counts": invalid_value_counts,
            "allowed_values": allowed_values
        }

        if invalid_count == 0:
            passed_columns[column] = column_result
        elif invalid_rate > invalid_rate_threshold:
            failed_columns[column] = column_result
        else:
            warning_columns[column] = column_result

    return {
        "passed_columns": passed_columns,
        "warning_columns": warning_columns,
        "failed_columns": failed_columns,
        "skipped_columns": skipped_columns
    }


def run_category_checks(
    df: pd.DataFrame,
    allowed_categories: dict = None,
    baseline_df: pd.DataFrame = None,
    categorical_columns: list = None,
    invalid_rate_threshold: float = DEFAULT_INVALID_CATEGORY_RATE_THRESHOLD,
    include_numeric_low_cardinality: bool = False,
    max_unique_values: int = 20
) -> dict:

    if allowed_categories is None:
        if baseline_df is None:
            raise ValueError("You must provide either allowed_categories or baseline_df.")

        allowed_categories = infer_allowed_categories_from_baseline(
            baseline_df=baseline_df,
            categorical_columns=categorical_columns,
            include_numeric_low_cardinality=include_numeric_low_cardinality,
            max_unique_values=max_unique_values
        )

    details = calculate_invalid_category_details(
        df=df,
        allowed_categories=allowed_categories,
        invalid_rate_threshold=invalid_rate_threshold
    )

    if len(details["failed_columns"]) > 0:
        status = "FAIL"
    elif len(details["warning_columns"]) > 0 or len(details["skipped_columns"]) > 0:
        status = "WARNING"
    else:
        status = "PASS"

    return {
        "check": "category_checks",
        "status": status,
        "allowed_categories": allowed_categories,
        "invalid_rate_threshold": invalid_rate_threshold,
        "details": details
    }