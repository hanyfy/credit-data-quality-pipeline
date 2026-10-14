"""Trace le nombre de lignes en anomalie par règle à partir de reports/dq_report.json."""
import json
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

src, dst = (sys.argv + [None, None])[1:3]
r = json.load(open(src or "reports/dq_report.json"))
items = sorted(r["per_rule"].items(), key=lambda kv: kv[1])
fig, ax = plt.subplots(figsize=(7, 3.8))
ax.barh([k for k, _ in items], [v for _, v in items], color="#1f5fbf")
ax.set(xlabel="Lignes en anomalie",
       title=f"Rapport qualité : {r['total_rows']:,} lignes, {r['anomaly_pct']} % en anomalie".replace(",", " "))
fig.tight_layout(); fig.savefig(dst or "docs/anomalies_per_rule.png", dpi=130)
