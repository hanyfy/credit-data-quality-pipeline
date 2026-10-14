"""Point d'entrée : lit un parquet, exécute les contrôles, écrit un rapport JSON + les anomalies."""
import argparse
import json
from pathlib import Path

from dq.checks import collect_anomalies, get_spark, run_checks


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/loans.parquet")
    ap.add_argument("--report", default="reports/dq_report.json")
    ap.add_argument("--anomalies", default="reports/anomalies")
    ap.add_argument("--max-anomaly-pct", type=float, default=10.0,
                    help="code retour 1 si le % de lignes en anomalie dépasse ce seuil")
    a = ap.parse_args()

    spark = get_spark()
    df = spark.read.parquet(a.input).cache()
    total = df.count()
    per_rule = run_checks(df)
    bad_rows = collect_anomalies(df).drop("rule").distinct().count()
    pct = round(100 * bad_rows / total, 3) if total else 0.0

    report = {"total_rows": total, "rows_with_anomaly": bad_rows,
              "anomaly_pct": pct, "per_rule": per_rule}
    Path(a.report).parent.mkdir(parents=True, exist_ok=True)
    Path(a.report).write_text(json.dumps(report, indent=2))
    collect_anomalies(df).write.mode("overwrite").parquet(a.anomalies)
    print(json.dumps(report, indent=2))
    spark.stop()
    return 1 if pct > a.max_anomaly_pct else 0


if __name__ == "__main__":
    raise SystemExit(main())
