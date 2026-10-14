"""Contrôles qualité de données en PySpark.

Chaque règle est une fonction pure : DataFrame -> DataFrame des lignes en anomalie
(avec une colonne `rule`). Cela les rend testables unitairement.
"""
from functools import reduce
from typing import Callable, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ALLOWED_PRODUCTS = ["consumer", "auto", "mortgage", "sme"]
REQUIRED_COLUMNS = ["loan_id", "customer_age", "monthly_income", "loan_amount",
                    "interest_rate", "product", "status", "issue_date", "maturity_date"]


def _tag(df: DataFrame, cond, rule: str) -> DataFrame:
    return df.filter(cond).withColumn("rule", F.lit(rule))


def missing_values(df: DataFrame) -> DataFrame:
    """null, et NaN pour les colonnes flottantes (Spark distingue les deux ; un NaN venu de pandas n'est pas null)."""
    float_cols = {c for c, t in df.dtypes if t in ("double", "float")}
    conds = [F.col(c).isNull() | (F.isnan(F.col(c)) if c in float_cols else F.lit(False))
             for c in REQUIRED_COLUMNS]
    return _tag(df, reduce(lambda a, b: a | b, conds), "missing_value")


def age_out_of_range(df: DataFrame) -> DataFrame:
    return _tag(df, (F.col("customer_age") < 18) | (F.col("customer_age") > 100), "age_out_of_range")


def rate_out_of_range(df: DataFrame) -> DataFrame:
    return _tag(df, (F.col("interest_rate") < 0) | (F.col("interest_rate") > 60), "rate_out_of_range")


def non_positive_amount(df: DataFrame) -> DataFrame:
    return _tag(df, F.col("loan_amount") <= 0, "non_positive_amount")


def unknown_product(df: DataFrame) -> DataFrame:
    return _tag(df, ~F.col("product").isin(ALLOWED_PRODUCTS), "unknown_product")


def maturity_before_issue(df: DataFrame) -> DataFrame:
    return _tag(df, F.col("maturity_date") < F.col("issue_date"), "maturity_before_issue")


def income_outlier(df: DataFrame, k: float = 3.0) -> DataFrame:
    """Revenus aberrants : règle de l'écart interquartile (IQR) appliquée sur le log du revenu.
    Le revenu est très asymétrique ; le log le rend proche d'une loi symétrique. Bornes : Q1 - k*IQR, Q3 + k*IQR.
    Quantiles approchés (approxQuantile), calculés sur les revenus strictement positifs."""
    # NaN > 0 vaut vrai en Spark (NaN est « plus grand » que tout nombre) : on l'exclut explicitement
    positive = (F.col("monthly_income") > 0) & ~F.isnan("monthly_income")
    valid = df.filter(positive).select(F.log("monthly_income").alias("v"))
    q1, q3 = valid.approxQuantile("v", [0.25, 0.75], 0.001)
    iqr = q3 - q1
    low, high = q1 - k * iqr, q3 + k * iqr
    lg = F.log(F.col("monthly_income"))
    return _tag(df, positive & ((lg < low) | (lg > high)), "income_outlier")


def duplicates(df: DataFrame) -> DataFrame:
    """Lignes dont tous les champs sont identiques à une autre ligne (toutes les occurrences)."""
    cnt = df.groupBy(*df.columns).count().filter(F.col("count") > 1).drop("count")
    return df.join(cnt, on=df.columns, how="inner").withColumn("rule", F.lit("duplicate_row"))


RULES: Dict[str, Callable[[DataFrame], DataFrame]] = {
    "missing_value": missing_values,
    "age_out_of_range": age_out_of_range,
    "rate_out_of_range": rate_out_of_range,
    "non_positive_amount": non_positive_amount,
    "unknown_product": unknown_product,
    "maturity_before_issue": maturity_before_issue,
    "income_outlier": income_outlier,
    "duplicate_row": duplicates,
}


def run_checks(df: DataFrame) -> Dict[str, int]:
    """Nombre de lignes en anomalie par règle (une ligne peut violer plusieurs règles)."""
    return {name: rule(df).count() for name, rule in RULES.items()}


def collect_anomalies(df: DataFrame) -> DataFrame:
    return reduce(DataFrame.unionByName, [rule(df) for rule in RULES.values()])


def get_spark(app: str = "credit-dq") -> SparkSession:
    return (SparkSession.builder.master("local[*]").appName(app)
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false").getOrCreate())
