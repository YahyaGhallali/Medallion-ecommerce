"""
Analysis: Monthly Payment Revenue Plots
Reads the gold-layer payments_per_month_dataset.parquet and creates visualizations.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ── Load data ──────────────────────────────────────────────────────────────
df = pd.read_parquet("./data/gold/payments_per_month_dataset.parquet")

# Clean: drop rows where month is null, sort chronologically
df = df.dropna(subset=["month"])
df = df.sort_values("month").reset_index(drop=True)

# Derived columns
df["Sum_k"] = df["Sum"] / 1000  # revenue in thousands
df["MoM_growth"] = df["Sum"].pct_change() * 100  # month-over-month % growth

print(df.to_string(index=False))
print(f"\nTotal revenue: R$ {df['Sum'].sum():,.0f}")

# ── Plot style ─────────────────────────────────────────────────────────────
plt.style.use("seaborn-v0_8-darkgrid")
fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 2]})
fig.suptitle("Olist — Monthly Payment Revenue", fontsize=16, fontweight="bold", y=0.95)

# ── Plot 1: Monthly Revenue Bar Chart ──────────────────────────────────────
ax1 = axes[0]
colors = ["#2ecc71" if g >= 0 else "#e74c3c" for g in df["MoM_growth"].fillna(0)]
bars = ax1.bar(df["month"], df["Sum_k"], color=colors, edgecolor="white", linewidth=0.5)
ax1.set_ylabel("Revenue (R$ thousands)", fontsize=12)
ax1.set_title("Monthly Revenue", fontsize=13)
ax1.tick_params(axis="x", rotation=45)
ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}k"))

# Add value labels on top of bars
for bar, val in zip(bars, df["Sum_k"]):
    ax1.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 15,
        f"{val:,.0f}k",
        ha="center",
        va="bottom",
        fontsize=7,
        fontweight="bold",
    )

# ── Plot 2: Month-over-Month Growth ───────────────────────────────────────
ax2 = axes[1]
growth_colors = ["#2ecc71" if g >= 0 else "#e74c3c" for g in df["MoM_growth"].fillna(0)]
ax2.bar(df["month"], df["MoM_growth"].fillna(0), color=growth_colors, edgecolor="white", linewidth=0.5)
ax2.axhline(y=0, color="gray", linewidth=0.8, linestyle="--")
ax2.set_ylabel("MoM Growth (%)", fontsize=12)
ax2.set_xlabel("Month", fontsize=12)
ax2.set_title("Month-over-Month Growth Rate", fontsize=13)
ax2.tick_params(axis="x", rotation=45)
ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:+.0f}%"))

plt.tight_layout()
plt.savefig("./data/gold/payments_monthly_analysis.png", dpi=150, bbox_inches="tight")
plt.show()
print("\n✓ Chart saved to ./data/gold/payments_monthly_analysis.png")
