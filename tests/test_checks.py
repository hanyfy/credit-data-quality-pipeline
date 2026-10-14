from datetime import date

from dq import checks

COLS = checks.REQUIRED_COLUMNS


def row(**over):
    base = dict(loan_id=1, customer_age=35, monthly_income=2500.0, loan_amount=10000.0,
                interest_rate=8.5, product="consumer", status="current",
                issue_date=date(2022, 1, 1), maturity_date=date(2025, 1, 1))
    base.update(over)
    return tuple(base[c] for c in COLS)


def make(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_clean_row_has_no_anomaly(spark):
    df = make(spark, [row()])
    assert all(v == 0 for v in checks.run_checks(df).values())


def test_each_rule_detects_its_case(spark):
    df = make(spark, [
        row(loan_id=1, monthly_income=None),
        row(loan_id=2, customer_age=7),
        row(loan_id=3, interest_rate=99.0),
        row(loan_id=4, loan_amount=-5.0),
        row(loan_id=5, product="xyz"),
        row(loan_id=6, maturity_date=date(2021, 1, 1)),
    ])
    r = checks.run_checks(df)
    assert r["missing_value"] == 1
    assert r["age_out_of_range"] == 1
    assert r["rate_out_of_range"] == 1
    assert r["non_positive_amount"] == 1
    assert r["unknown_product"] == 1
    assert r["maturity_before_issue"] == 1
    assert r["duplicate_row"] == 0


def test_duplicates_counts_every_occurrence(spark):
    df = make(spark, [row(loan_id=1), row(loan_id=1), row(loan_id=2)])
    assert checks.run_checks(df)["duplicate_row"] == 2


def test_boundaries_are_valid(spark):
    df = make(spark, [row(customer_age=18), row(loan_id=2, customer_age=100),
                      row(loan_id=3, interest_rate=0.0), row(loan_id=4, interest_rate=60.0)])
    assert checks.run_checks(df)["age_out_of_range"] == 0
    assert checks.run_checks(df)["rate_out_of_range"] == 0
