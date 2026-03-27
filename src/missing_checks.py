import pandas as pd

DEFAULT_MISSING_THRESHOLD = 0.10


def calculate_missing_percentages(df: pd.DataFrame) -> dict:
    missing_percentages = (df.isnull().mean()).to_dict()
    return {col: round(percent, 4) for col, percent in missing_percentages.items()}


def calculate_missing_counts(df: pd.DataFrame) -> dict:
    return df.isnull().sum().to_dict()


def resolve_missing_thresholds(df: pd.DataFrame, thresholds=None,
                               default_threshold: float = DEFAULT_MISSING_THRESHOLD) -> dict:
    resolved = {}

    for column in df.columns:
        if thresholds and column in thresholds:
            resolved[column] = thresholds[column]
        else:
            resolved[column] = default_threshold

    return resolved


def check_missing_thresholds(df: pd.DataFrame, thresholds=None,
                             default_threshold: float = DEFAULT_MISSING_THRESHOLD) -> dict:
    missing_percentages = calculate_missing_percentages(df)
    missing_counts = calculate_missing_counts(df)
    resolved_thresholds = resolve_missing_thresholds(df, thresholds, default_threshold)

    failed_columns = {}

    for column, missing_percentage in missing_percentages.items():
        threshold = resolved_thresholds[column]
        if missing_percentage > threshold:
            failed_columns[column] = {
                "missing_percentage": missing_percentage,
                "missing_count": missing_counts[column],
                "threshold": threshold
            }

    return {
        "check": "missing_values",
        "status": "PASS" if len(failed_columns) == 0 else "FAIL",
        "missing_percentages": missing_percentages,
        "missing_counts": missing_counts,
        "thresholds_used": resolved_thresholds,
        "failed_columns": failed_columns
    }


def run_missing_checks(df: pd.DataFrame, thresholds=None,
                       default_threshold: float = DEFAULT_MISSING_THRESHOLD) -> dict:
    return check_missing_thresholds(df, thresholds, default_threshold)