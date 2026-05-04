import pandas as pd


def check_numeric_rules(df: pd.DataFrame, numeric_rules: dict = None) -> dict:
    if numeric_rules is None:
        numeric_rules = {}

    failed_columns = {}
    skipped_columns = []

    for column, rules in numeric_rules.items():
        if column not in df.columns:
            skipped_columns.append(column)
            continue

        if not pd.api.types.is_numeric_dtype(df[column]):
            failed_columns[column] = {
                "reason": "Column is not numeric.",
                "expected_rule": rules,
                "actual_dtype": str(df[column].dtype)
            }
            continue

        non_missing_series = df[column].dropna()
        violations = {}

        if "min" in rules:
            min_value = rules["min"]
            below_min = non_missing_series[non_missing_series < min_value]

            if len(below_min) > 0:
                violations["below_min"] = {
                    "min_allowed": min_value,
                    "violation_count": int(len(below_min)),
                    "sample_values": below_min.head(10).tolist()
                }

        if "max" in rules:
            max_value = rules["max"]
            above_max = non_missing_series[non_missing_series > max_value]

            if len(above_max) > 0:
                violations["above_max"] = {
                    "max_allowed": max_value,
                    "violation_count": int(len(above_max)),
                    "sample_values": above_max.head(10).tolist()
                }

        if violations:
            failed_columns[column] = {
                "rule": rules,
                "violations": violations
            }

    if failed_columns:
        status = "FAIL"
    elif skipped_columns:
        status = "WARNING"
    else:
        status = "PASS"

    return {
        "check": "numeric_rules",
        "status": status,
        "failed_columns": failed_columns,
        "skipped_columns": skipped_columns
    }


def check_allowed_value_rules(df: pd.DataFrame, allowed_value_rules: dict = None) -> dict:
    if allowed_value_rules is None:
        allowed_value_rules = {}

    failed_columns = {}
    skipped_columns = []

    for column, allowed_values in allowed_value_rules.items():
        if column not in df.columns:
            skipped_columns.append(column)
            continue

        allowed_set = set(allowed_values)
        non_missing_series = df[column].dropna()

        invalid_series = non_missing_series[~non_missing_series.isin(allowed_set)]

        if len(invalid_series) > 0:
            invalid_value_counts = {
                str(value): int(count)
                for value, count in invalid_series.value_counts().items()
            }

            failed_columns[column] = {
                "allowed_values": allowed_values,
                "invalid_count": int(len(invalid_series)),
                "invalid_values": sorted(list(invalid_value_counts.keys())),
                "invalid_value_counts": invalid_value_counts
            }

    if failed_columns:
        status = "FAIL"
    elif skipped_columns:
        status = "WARNING"
    else:
        status = "PASS"

    return {
        "check": "allowed_value_rules",
        "status": status,
        "failed_columns": failed_columns,
        "skipped_columns": skipped_columns
    }


def check_date_rules(df: pd.DataFrame, date_rules: dict = None) -> dict:
    if date_rules is None:
        date_rules = {}

    failed_columns = {}
    skipped_columns = []

    current_timestamp = pd.Timestamp.now()

    for column, rules in date_rules.items():
        if column not in df.columns:
            skipped_columns.append(column)
            continue

        parsed_dates = pd.to_datetime(df[column], errors="coerce")
        original_non_missing = df[column].dropna()

        invalid_parse_count = int(parsed_dates.isna().sum() - df[column].isna().sum())

        violations = {}

        if invalid_parse_count > 0:
            violations["invalid_dates"] = {
                "violation_count": invalid_parse_count
            }

        non_missing_dates = parsed_dates.dropna()

        if rules.get("not_future", False):
            future_dates = non_missing_dates[non_missing_dates > current_timestamp]

            if len(future_dates) > 0:
                violations["future_dates"] = {
                    "violation_count": int(len(future_dates)),
                    "sample_values": future_dates.head(10).astype(str).tolist()
                }

        if "min_date" in rules:
            min_date = pd.to_datetime(rules["min_date"])
            dates_before_min = non_missing_dates[non_missing_dates < min_date]

            if len(dates_before_min) > 0:
                violations["before_min_date"] = {
                    "min_date_allowed": str(min_date.date()),
                    "violation_count": int(len(dates_before_min)),
                    "sample_values": dates_before_min.head(10).astype(str).tolist()
                }

        if "max_date" in rules:
            max_date = pd.to_datetime(rules["max_date"])
            dates_after_max = non_missing_dates[non_missing_dates > max_date]

            if len(dates_after_max) > 0:
                violations["after_max_date"] = {
                    "max_date_allowed": str(max_date.date()),
                    "violation_count": int(len(dates_after_max)),
                    "sample_values": dates_after_max.head(10).astype(str).tolist()
                }

        if violations:
            failed_columns[column] = {
                "rule": rules,
                "violations": violations
            }

    if failed_columns:
        status = "FAIL"
    elif skipped_columns:
        status = "WARNING"
    else:
        status = "PASS"

    return {
        "check": "date_rules",
        "status": status,
        "failed_columns": failed_columns,
        "skipped_columns": skipped_columns
    }


def run_rule_checks(df: pd.DataFrame, numeric_rules: dict = None, allowed_value_rules: dict = None,
                    date_rules: dict = None) -> dict:
    numeric_result = check_numeric_rules(df, numeric_rules)
    allowed_values_result = check_allowed_value_rules(df, allowed_value_rules)
    date_result = check_date_rules(df, date_rules)

    statuses = [
        numeric_result["status"],
        allowed_values_result["status"],
        date_result["status"]
    ]

    if "FAIL" in statuses:
        overall_status = "FAIL"
    elif "WARNING" in statuses:
        overall_status = "WARNING"
    else:
        overall_status = "PASS"

    return {
        "check": "rule_checks",
        "status": overall_status,
        "details": {
            "numeric_rules": numeric_result,
            "allowed_value_rules": allowed_values_result,
            "date_rules": date_result
        }
    }