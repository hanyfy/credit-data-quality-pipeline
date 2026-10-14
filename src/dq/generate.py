"""Génère un jeu de données de prêts 100 % synthétique, reproductible (seed fixe).

Des anomalies sont injectées à taux connu : les tests peuvent ainsi vérifier
que les contrôles retrouvent exactement ce qui a été injecté.
Aucune donnée réelle n'est utilisée.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

PRODUCTS = ["consumer", "auto", "mortgage", "sme"]
STATUSES = ["current", "late", "default", "closed"]


def generate(n_rows: int = 100_000, seed: int = 42, anomaly_rate: float = 0.02) -> pd.DataFrame:
    """Retourne un DataFrame pandas. `anomaly_rate` = part de lignes touchée par CHAQUE type d'anomalie."""
    rng = np.random.default_rng(seed)
    issue = pd.Timestamp("2018-01-01") + pd.to_timedelta(rng.integers(0, 365 * 6, n_rows), unit="D")
    term_months = rng.choice([12, 24, 36, 60, 120], n_rows)
    df = pd.DataFrame({
        "loan_id": np.arange(1, n_rows + 1),
        "customer_age": rng.integers(18, 80, n_rows),
        "monthly_income": rng.lognormal(mean=7.8, sigma=0.5, size=n_rows).round(2),
        "loan_amount": rng.lognormal(mean=9.0, sigma=0.8, size=n_rows).round(2),
        "interest_rate": rng.uniform(2, 25, n_rows).round(2),
        "product": rng.choice(PRODUCTS, n_rows, p=[0.5, 0.2, 0.15, 0.15]),
        "status": rng.choice(STATUSES, n_rows, p=[0.7, 0.15, 0.05, 0.1]),
        "issue_date": issue,
    })
    df["maturity_date"] = df["issue_date"] + pd.to_timedelta(term_months * 30, unit="D")

    k = int(n_rows * anomaly_rate)
    pick = lambda: rng.choice(n_rows, k, replace=False)  # noqa: E731
    df.loc[pick(), "monthly_income"] = np.nan                        # valeurs manquantes
    df.loc[pick(), "customer_age"] = rng.choice([5, 12, 150, 200], k)  # âges impossibles
    df.loc[pick(), "interest_rate"] = rng.choice([-3.0, 75.0, 120.0], k)  # taux hors bornes
    df.loc[pick(), "loan_amount"] = -df["loan_amount"].abs().iloc[0]  # montants négatifs
    df.loc[pick(), "product"] = "unknown_product"                    # modalité inconnue
    idx = pick()
    df.loc[idx, "maturity_date"] = df.loc[idx, "issue_date"] - pd.Timedelta(days=30)  # échéance < émission
    dup = df.sample(k, random_state=seed)
    return pd.concat([df, dup], ignore_index=True)                   # doublons exacts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/loans.parquet")
    a = ap.parse_args()
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Spark 3.5 ne lit pas les timestamps en nanosecondes : on force les microsecondes
    generate(a.rows, a.seed).to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    print(f"écrit {out}")


if __name__ == "__main__":
    main()
