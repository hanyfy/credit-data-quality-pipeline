"""Génère des données avec anomalies injectées à taux connu et vérifie que les contrôles les retrouvent."""
from dq import checks
from dq.generate import generate

N, RATE = 5_000, 0.02
K = int(N * RATE)  # lignes touchées par chaque anomalie


def test_injected_anomalies_are_found(spark):
    pdf = generate(n_rows=N, seed=1, anomaly_rate=RATE)
    r = checks.run_checks(spark.createDataFrame(pdf))
    # chaque anomalie est injectée sur K lignes ; la copie des doublons peut en reproduire quelques-unes
    hi = int(K * 1.2)
    for rule in ("missing_value", "unknown_product", "non_positive_amount", "maturity_before_issue"):
        assert K <= r[rule] <= hi, rule
    assert r["duplicate_row"] >= K  # chaque doublon crée au moins 2 occurrences


def test_generation_is_reproducible():
    a = generate(n_rows=500, seed=7)
    b = generate(n_rows=500, seed=7)
    assert a.equals(b)
