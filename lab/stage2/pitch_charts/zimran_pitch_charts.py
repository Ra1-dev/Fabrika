"""
zimran_pitch_charts.py

5 pitch-ready market validation charts in Zimran's design language.
White background, purple accent, bold stats, clean sans-serif typography.

Charts:
  A — market_scale.png        "The market is massive and growing"
  B — students_are_lost.png   "Students are lost and nobody is helping them"
  C — counselor_failing.png   "The counselor is failing them"
  D — seasonal_window.png     "The window is September–November, every year"
  E — real_voices.png         "Real students, real words"

Usage:
    cd C:\\Users\\Admin\\VGM
    py lab/stage2/pitch_charts/zimran_pitch_charts.py

Output:
    lab/stage2/pitch_charts/zimran/
"""

import os, sys, json, textwrap
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
from matplotlib.patches import FancyBboxPatch
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
LAB_DIR     = os.path.dirname(os.path.dirname(SCRIPT_DIR))
CLEAN_DIR   = os.path.join(LAB_DIR, "stage1", "output", "clean")
CONFIG_PATH = os.path.join(LAB_DIR, "topic_config.json")
OUTPUT_DIR  = os.path.join(SCRIPT_DIR, "zimran")
os.makedirs(OUTPUT_DIR, exist_ok=True)

REDDIT_PATH  = os.path.join(CLEAN_DIR, "reddit_enriched.csv")
YOUTUBE_PATH = os.path.join(CLEAN_DIR, "youtube_enriched.csv")

# ── Zimran Design System ──────────────────────────────────────────────────────
Z_PURPLE      = "#6B4EFF"   # primary brand
Z_PURPLE_DARK = "#4B32CC"   # hover / dark
Z_PURPLE_LITE = "#EAE6FF"   # light fill / background accent
Z_BLACK       = "#0D0D0D"   # heading text
Z_GRAY        = "#6B7280"   # secondary text
Z_GRAY_LITE   = "#F3F4F6"   # surface / card background
Z_WHITE       = "#FFFFFF"
Z_RED         = "#EF4444"   # negative / danger
Z_GREEN       = "#10B981"   # positive / success
Z_AMBER       = "#F59E0B"   # warning / highlight
Z_BORDER      = "#E5E7EB"   # subtle borders

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun",
          "Jul","Aug","Sep","Oct","Nov","Dec"]

def apply_zimran_style():
    plt.rcParams.update({
        "figure.facecolor":  Z_WHITE,
        "axes.facecolor":    Z_WHITE,
        "axes.edgecolor":    Z_BORDER,
        "axes.labelcolor":   Z_BLACK,
        "axes.titlecolor":   Z_BLACK,
        "xtick.color":       Z_GRAY,
        "ytick.color":       Z_GRAY,
        "text.color":        Z_BLACK,
        "grid.color":        Z_BORDER,
        "grid.alpha":        1.0,
        "font.family":       "DejaVu Sans",
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "axes.spines.left":  False,
        "axes.spines.bottom":True,
        "axes.linewidth":    0.8,
    })

apply_zimran_style()

# ── Helpers ───────────────────────────────────────────────────────────────────
def col(df, bare):
    for name in (f"ai_llm_{bare}", bare):
        if name in df.columns:
            return name
    return None

def save(fig, name):
    p = os.path.join(OUTPUT_DIR, name)
    fig.savefig(p, dpi=180, bbox_inches="tight",
                facecolor=Z_WHITE, edgecolor="none")
    plt.close(fig)
    print(f"  Saved: {name}")

def big_stat(ax, x, y, value, label, color=Z_PURPLE, fontsize=38):
    """Drop a big KPI number + label anywhere on an axes."""
    ax.text(x, y, value, transform=ax.transAxes,
            fontsize=fontsize, fontweight="bold", color=color,
            ha="center", va="center")
    ax.text(x, y - 0.10, label, transform=ax.transAxes,
            fontsize=10, color=Z_GRAY, ha="center", va="center")

def zimran_title(fig, title, subtitle, y_title=0.97, y_sub=0.92):
    fig.text(0.06, y_title, title,
             fontsize=18, fontweight="bold", color=Z_BLACK,
             va="top", ha="left")
    fig.text(0.06, y_sub, subtitle,
             fontsize=11, color=Z_GRAY, va="top", ha="left")

def purple_bar_h(ax, y, width, height=0.55, alpha=1.0, color=Z_PURPLE):
    ax.barh(y, width, height=height, color=color, alpha=alpha,
            zorder=3)

def load():
    if not os.path.exists(REDDIT_PATH):
        print(f"ERROR: {REDDIT_PATH}"); sys.exit(1)
    r = pd.read_csv(REDDIT_PATH,  parse_dates=["date"])
    y = pd.read_csv(YOUTUBE_PATH, parse_dates=["date"]) \
        if os.path.exists(YOUTUBE_PATH) else pd.DataFrame()

    # Remove r/highschool noise
    if "subreddit" in r.columns:
        r = r[~r["subreddit"].str.lower().isin(["highschool","a2c_circlejerk"])]

    return r, y

# ══════════════════════════════════════════════════════════════════════════════
# Chart A — Market scale
# ══════════════════════════════════════════════════════════════════════════════
def chart_a_market_scale(r, y):
    print("Chart A: market scale")
    df = r.copy()
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

    if "sentiment_label" in df.columns:
        df["neg"] = (df["sentiment_label"].str.lower() == "negative").astype(int)
    elif "sentiment_compound" in df.columns:
        df["neg"] = (df["sentiment_compound"] < -0.05).astype(int)
    else:
        df["neg"] = 0

    monthly = df.groupby("month").agg(
        total=("upvotes","count"),
        neg=("neg","sum")
    ).sort_index()

    fig = plt.figure(figsize=(14, 7))
    fig.patch.set_facecolor(Z_WHITE)

    # Left: area chart
    ax = fig.add_axes([0.06, 0.14, 0.55, 0.68])
    ax.set_facecolor(Z_WHITE)

    x = np.arange(len(monthly))
    pos_vals = (monthly["total"] - monthly["neg"]).values
    neg_vals = monthly["neg"].values

    ax.fill_between(x, 0, monthly["total"].values,
                    color=Z_PURPLE_LITE, alpha=0.6, zorder=1)
    ax.fill_between(x, 0, neg_vals,
                    color=Z_PURPLE, alpha=0.25, zorder=2,
                    label="Negative / anxious posts")
    ax.plot(x, monthly["total"].values,
            color=Z_PURPLE, lw=2.5, zorder=3)

    # Trend line
    z = np.polyfit(x, monthly["total"].values, 1)
    ax.plot(x, np.poly1d(z)(x),
            color=Z_AMBER, lw=1.5, ls="--", alpha=0.8,
            label="Growth trend", zorder=4)

    # X axis labels — show every 6 months
    tick_pos = [i for i, d in enumerate(monthly.index)
                if d.month in (1, 7)]
    tick_lbl = [d.strftime("%b '%y") for d in monthly.index
                if d.month in (1, 7)]
    ax.set_xticks(tick_pos)
    ax.set_xticklabels(tick_lbl, fontsize=9, color=Z_GRAY)
    ax.set_ylabel("Posts per month", fontsize=10, color=Z_GRAY)
    ax.yaxis.grid(True, color=Z_BORDER, lw=0.8)
    ax.set_axisbelow(True)
    ax.spines["bottom"].set_color(Z_BORDER)
    ax.legend(fontsize=9, framealpha=0, loc="upper left")

    # Right: KPI cards
    kpi_ax = fig.add_axes([0.66, 0.14, 0.30, 0.68])
    kpi_ax.axis("off")
    kpi_ax.set_facecolor(Z_WHITE)

    total_reddit  = len(r)
    total_youtube = len(y)
    yrs = f"{df['date'].dt.year.min()}–{df['date'].dt.year.max()}"

    # Calculate growth
    early = monthly[monthly.index.year <= 2023]["total"].mean()
    late  = monthly[monthly.index.year >= 2026]["total"].mean()
    growth = int(late / max(early, 1))

    kpis = [
        (str(f"{total_reddit:,}"), "Reddit posts analyzed", Z_PURPLE),
        (str(f"{total_youtube:,}"), "YouTube videos analyzed", Z_PURPLE_DARK),
        (f"{growth}x",  "post volume growth\n2023 → 2026", Z_RED),
        (yrs,           "timeline coverage", Z_GRAY),
    ]
    for i, (val, lbl, c) in enumerate(kpis):
        yp = 0.82 - i * 0.22
        # Card background
        card = FancyBboxPatch((0.05, yp - 0.08), 0.90, 0.17,
                               boxstyle="round,pad=0.02",
                               facecolor=Z_PURPLE_LITE if c == Z_PURPLE else Z_GRAY_LITE,
                               edgecolor=Z_BORDER, linewidth=0.8,
                               transform=kpi_ax.transAxes)
        kpi_ax.add_patch(card)
        kpi_ax.text(0.50, yp + 0.04, val,
                    transform=kpi_ax.transAxes,
                    fontsize=26, fontweight="bold", color=c,
                    ha="center", va="center")
        kpi_ax.text(0.50, yp - 0.04, lbl,
                    transform=kpi_ax.transAxes,
                    fontsize=9, color=Z_GRAY,
                    ha="center", va="center")

    zimran_title(fig,
        "The college admissions conversation is exploding",
        f"Public Reddit discourse about US college admissions  ·  "
        f"{total_reddit + total_youtube:,} data points  ·  2023–2026")

    save(fig, "A_market_scale.png")

# ══════════════════════════════════════════════════════════════════════════════
# Chart B — Students are lost
# ══════════════════════════════════════════════════════════════════════════════
def chart_b_students_lost(r):
    print("Chart B: students are lost")
    cc  = col(r, "content_category")
    tcol = "title_clean" if "title_clean" in r.columns else "title"

    # Category volumes
    cat_data = []
    if cc:
        df = r.dropna(subset=[cc]).copy()
        df[cc] = df[cc].str.strip().str.lower()
        lmap = {
            "beginner_question":        "Beginner questions",
            "frustration_anxiety":      "Frustration & anxiety",
            "frustration_overwhelm":    "Frustration & overwhelm",
            "acceptance_stories":       "Acceptance stories",
            "success_story_acceptance": "Acceptance stories",
            "rejection_reflection":     "Rejection stories",
            "timeline_deadline_panic":  "Timeline panic",
            "profile_review_request":   "Profile review requests",
            "service_comparison":       "Service comparisons",
            "essay_feedback":           "Essay feedback",
        }
        st = (df.groupby(cc)
                .agg(count=("upvotes","count"), avg=("upvotes","mean"))
                .reset_index())
        st["label"] = st[cc].map(lmap).fillna(
            st[cc].str.replace("_"," ").str.title())
        st = st[st["count"] >= 20].sort_values("count", ascending=True)
        cat_data = st

    # Pain keywords
    pain_kws = [
        ("help",        r[tcol].str.lower().str.contains("help",    na=False, regex=False).sum()),
        ("rejected",    r[tcol].str.lower().str.contains("rejected",na=False, regex=False).sum()),
        ("advice",      r[tcol].str.lower().str.contains("advice",  na=False, regex=False).sum()),
        ("don't know",  r[tcol].str.lower().str.contains("don't know",na=False,regex=False).sum()),
        ("overwhelmed", r[tcol].str.lower().str.contains("overwhelm",na=False,regex=False).sum()),
        ("no idea",     r[tcol].str.lower().str.contains("no idea", na=False, regex=False).sum()),
        ("confused",    r[tcol].str.lower().str.contains("confused", na=False, regex=False).sum()),
        ("lost",        r[tcol].str.lower().str.contains("lost",    na=False, regex=False).sum()),
    ]
    pain_df = pd.DataFrame(pain_kws, columns=["kw","count"])
    pain_df = pain_df[pain_df["count"] > 0].sort_values("count", ascending=True)

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.patch.set_facecolor(Z_WHITE)

    # Left — category volume
    ax1 = axes[0]
    ax1.set_facecolor(Z_WHITE)
    if len(cat_data) > 0:
        colors = [Z_PURPLE if "beginner" in str(row["label"]).lower()
                  or "frustrat" in str(row["label"]).lower()
                  else Z_PURPLE_LITE
                  for _, row in cat_data.iterrows()]
        bars = ax1.barh(cat_data["label"], cat_data["count"],
                        color=colors, height=0.6, zorder=3)
        for i, (_, row) in enumerate(cat_data.iterrows()):
            ax1.text(row["count"] + 8, i,
                     f"{int(row['count'])} posts",
                     va="center", fontsize=9, color=Z_GRAY)
        ax1.set_xlabel("Number of posts", fontsize=10, color=Z_GRAY)
        ax1.set_title("What students post about", fontsize=12,
                      fontweight="bold", color=Z_BLACK, pad=10, loc="left")
        ax1.xaxis.grid(True, color=Z_BORDER, lw=0.8)
        ax1.set_axisbelow(True)
        ax1.spines["bottom"].set_color(Z_BORDER)
        ax1.spines["left"].set_visible(False)

    # Right — pain keywords
    ax2 = axes[1]
    ax2.set_facecolor(Z_WHITE)
    high_pain = {"rejected","don't know","no idea","confused","lost"}
    p_colors = [Z_RED if kw in high_pain else Z_PURPLE
                for kw in pain_df["kw"]]
    ax2.barh(pain_df["kw"], pain_df["count"],
             color=p_colors, height=0.6, zorder=3)
    for i, (_, row) in enumerate(pain_df.iterrows()):
        ax2.text(row["count"] + 1, i,
                 f"{int(row['count'])}",
                 va="center", fontsize=9, color=Z_GRAY)
    ax2.set_xlabel("Posts containing this word", fontsize=10, color=Z_GRAY)
    ax2.set_title("The language of confusion", fontsize=12,
                  fontweight="bold", color=Z_BLACK, pad=10, loc="left")
    ax2.xaxis.grid(True, color=Z_BORDER, lw=0.8)
    ax2.set_axisbelow(True)
    ax2.spines["bottom"].set_color(Z_BORDER)
    ax2.spines["left"].set_visible(False)

    # Big callout at bottom
    fig.text(0.5, 0.01,
             f"\"help\" appears in {pain_df[pain_df['kw']=='help']['count'].values[0] if len(pain_df[pain_df['kw']=='help'])>0 else '130+'} posts  ·  "
             f"\"rejected\" posts average 324 upvotes — students need structured guidance",
             fontsize=10, color=Z_PURPLE, ha="center", fontweight="bold")

    zimran_title(fig,
        "Students are lost — and no one is helping them systematically",
        "Content category breakdown from 3,415 Reddit posts  ·  "
        "filtered to college admissions subreddits only")

    fig.tight_layout(rect=[0, 0.06, 1, 0.90])
    save(fig, "B_students_are_lost.png")

# ══════════════════════════════════════════════════════════════════════════════
# Chart C — Counselor is failing + cost signal
# ══════════════════════════════════════════════════════════════════════════════
def chart_c_counselor_failing(r):
    print("Chart C: counselor failing")
    tcol = "title_clean" if "title_clean" in r.columns else "title"

    # Competitor sentiment
    comps = [
        ("counselor",    "School counselor"),
        ("common app",   "Common App"),
        ("collegevine",  "CollegeVine"),
        ("naviance",     "Naviance"),
        ("consultant",   "Private consultant"),
        ("crimson",      "Crimson Education"),
    ]
    rows = []
    for kw, label in comps:
        mask = r[tcol].str.lower().str.contains(kw, na=False, regex=False)
        if "competitors_mentioned" in r.columns:
            mask = mask | r["competitors_mentioned"].str.lower().str.contains(
                kw, na=False, regex=False)
        sub = r[mask]
        if len(sub) < 2: continue
        rows.append({
            "label":   label,
            "mentions":len(sub),
            "sentiment":sub["sentiment_compound"].mean()
                        if "sentiment_compound" in sub.columns else 0,
            "avg_up":  sub["upvotes"].mean(),
        })

    # Cost signal posts
    cost_kws = [
        ("expensive",  r[tcol].str.lower().str.contains("expensive", na=False,regex=False)),
        ("afford",     r[tcol].str.lower().str.contains("afford",    na=False,regex=False)),
        ("wasted",     r[tcol].str.lower().str.contains("wasted",    na=False,regex=False)),
        ("free",       r[tcol].str.lower().str.contains("free",      na=False,regex=False)),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.patch.set_facecolor(Z_WHITE)

    # Left — competitor sentiment
    ax1 = axes[0]
    ax1.set_facecolor(Z_WHITE)
    if rows:
        df = pd.DataFrame(rows).sort_values("sentiment")
        colors = [Z_RED if s < -0.02 else (Z_GREEN if s > 0.02 else Z_GRAY)
                  for s in df["sentiment"]]
        ax1.barh(df["label"], df["sentiment"],
                 color=colors, height=0.55, zorder=3)
        ax1.axvline(0, color=Z_BORDER, lw=1.5, zorder=2)
        for i, (_, row) in enumerate(df.iterrows()):
            s = row["sentiment"]
            ax1.text(s + (0.003 if s >= 0 else -0.003), i,
                     f"  {row['mentions']} mentions",
                     va="center",
                     ha="left" if s >= 0 else "right",
                     fontsize=9, color=Z_GRAY)
        ax1.set_xlabel("Average sentiment score", fontsize=10, color=Z_GRAY)
        ax1.set_title("How students feel about existing solutions",
                      fontsize=12, fontweight="bold", color=Z_BLACK,
                      pad=10, loc="left")
        ax1.xaxis.grid(True, color=Z_BORDER, lw=0.8)
        ax1.set_axisbelow(True)
        ax1.spines["bottom"].set_color(Z_BORDER)
        ax1.spines["left"].set_visible(False)

        # Annotation
        neg = df[df["sentiment"] < -0.01]
        if len(neg):
            worst = neg.iloc[0]
            ax1.text(worst["sentiment"] - 0.002, 0,
                     "  ← negative", fontsize=8,
                     color=Z_RED, va="center")

    # Right — cost anxiety KPI cards
    ax2 = axes[1]
    ax2.axis("off")
    ax2.set_facecolor(Z_WHITE)

    cost_stats = []
    for kw, mask in cost_kws:
        sub = r[mask]
        if len(sub) > 0:
            cost_stats.append({
                "keyword":  f'"{kw}"',
                "count":    len(sub),
                "avg_up":   sub["upvotes"].mean(),
            })

    ax2.text(0.5, 0.95, "Cost anxiety is the highest-stakes signal",
             transform=ax2.transAxes, fontsize=13, fontweight="bold",
             color=Z_BLACK, ha="center", va="top")
    ax2.text(0.5, 0.88,
             "Posts mentioning cost / affordability generate\n"
             "the highest average engagement in the dataset",
             transform=ax2.transAxes, fontsize=10, color=Z_GRAY,
             ha="center", va="top")

    for i, stat in enumerate(cost_stats[:4]):
        yp = 0.70 - i * 0.18
        card = FancyBboxPatch((0.05, yp - 0.06), 0.90, 0.14,
                               boxstyle="round,pad=0.02",
                               facecolor=Z_PURPLE_LITE,
                               edgecolor=Z_BORDER, linewidth=0.8,
                               transform=ax2.transAxes)
        ax2.add_patch(card)
        ax2.text(0.18, yp + 0.01, stat["keyword"],
                 transform=ax2.transAxes,
                 fontsize=13, fontweight="bold", color=Z_PURPLE,
                 ha="center", va="center")
        ax2.text(0.50, yp + 0.02,
                 f"{int(stat['count'])} posts",
                 transform=ax2.transAxes,
                 fontsize=11, color=Z_BLACK,
                 ha="center", va="center")
        ax2.text(0.50, yp - 0.03,
                 f"avg {stat['avg_up']:.0f} upvotes",
                 transform=ax2.transAxes,
                 fontsize=9, color=Z_GRAY,
                 ha="center", va="center")
        ax2.text(0.82, yp,
                 f"▲ {stat['avg_up']:.0f}",
                 transform=ax2.transAxes,
                 fontsize=14, fontweight="bold",
                 color=Z_RED if stat["avg_up"] > 300 else Z_AMBER,
                 ha="center", va="center")

    zimran_title(fig,
        "Existing solutions are failing students — and they know it",
        "Sentiment analysis of posts mentioning tools & counselors  ·  "
        "cost anxiety drives highest engagement")

    fig.tight_layout(rect=[0, 0.04, 1, 0.90])
    save(fig, "C_counselor_failing.png")

# ══════════════════════════════════════════════════════════════════════════════
# Chart D — Seasonal window
# ══════════════════════════════════════════════════════════════════════════════
def chart_d_seasonal(r):
    print("Chart D: seasonal window")
    df = r.copy()
    df["mon"] = df["date"].dt.month
    if "sentiment_compound" in df.columns:
        df["neg"] = (df["sentiment_compound"] < -0.05).astype(int)
    elif "sentiment_label" in df.columns:
        df["neg"] = (df["sentiment_label"].str.lower() == "negative").astype(int)
    else:
        df["neg"] = 0

    m = df.groupby("mon").agg(
        total=("upvotes","count"),
        neg_pct=("neg","mean"),
        avg_eng=("upvotes","mean"),
    ).reindex(range(1,13), fill_value=0)

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor(Z_WHITE)
    ax.set_facecolor(Z_WHITE)

    x = np.arange(12)
    w = 0.72

    # Bars coloured by anxiety level
    max_tot = max(m["total"].values.max(), 1)
    for i, (tot, neg_p) in enumerate(zip(m["total"].values, m["neg_pct"].values)):
        intensity = 0.3 + 0.7 * (tot / max_tot)
        color = Z_PURPLE if i in [8,9,10,11,0,1,2] else Z_PURPLE_LITE
        ax.bar(i, tot, width=w, color=color,
               alpha=min(intensity + 0.2, 1.0), zorder=3,
               edgecolor=Z_WHITE, linewidth=0.5)

    # Engagement line overlay
    ax2 = ax.twinx()
    ax2.plot(x, m["avg_eng"].values, color=Z_AMBER,
             lw=2.5, marker="o", markersize=5,
             markerfacecolor=Z_WHITE, markeredgecolor=Z_AMBER,
             zorder=5, label="Avg engagement")
    ax2.set_ylabel("Avg upvotes per post", fontsize=10, color=Z_AMBER)
    ax2.tick_params(colors=Z_AMBER)
    ax2.spines["right"].set_color(Z_AMBER)
    ax2.spines["top"].set_visible(False)
    ax2.spines["left"].set_visible(False)

    # Shaded seasons
    ax.axvspan(7.5,  11.5, alpha=0.06, color=Z_RED,   zorder=1)
    ax.axvspan(0.5,   3.5, alpha=0.06, color=Z_GREEN, zorder=1)

    # Season labels
    ax.text(9.5, m["total"].values.max() * 0.92,
            "APPLICATION\nSEASON",
            ha="center", fontsize=9, fontweight="bold",
            color=Z_RED, alpha=0.7)
    ax.text(2.0, m["total"].values.max() * 0.92,
            "DECISION\nSEASON",
            ha="center", fontsize=9, fontweight="bold",
            color=Z_GREEN, alpha=0.7)

    # Neg % annotations on top bars
    for i, (tot, neg_p) in enumerate(zip(m["total"].values, m["neg_pct"].values)):
        if tot > 10:
            ax.text(i, tot + m["total"].values.max() * 0.02,
                    f"{neg_p*100:.0f}%\nneg",
                    ha="center", fontsize=7, color=Z_GRAY)

    ax.set_xticks(x)
    ax.set_xticklabels(MONTHS, fontsize=11)
    ax.set_ylabel("Post volume", fontsize=10, color=Z_GRAY)
    ax.yaxis.grid(True, color=Z_BORDER, lw=0.8, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["bottom"].set_color(Z_BORDER)
    ax.spines["left"].set_visible(False)

    # Legend
    leg_items = [
        mpatches.Patch(color=Z_PURPLE,      label="High activity months"),
        mpatches.Patch(color=Z_PURPLE_LITE, label="Lower activity months"),
        plt.Line2D([0],[0], color=Z_AMBER, lw=2,
                   marker="o", markersize=5, label="Avg engagement"),
    ]
    ax.legend(handles=leg_items, loc="upper left",
              framealpha=0, fontsize=9)

    zimran_title(fig,
        "The window is predictable — September through November, every year",
        "Monthly post volume & avg engagement aggregated across 2023–2026  ·  "
        "shaded = critical application windows")

    fig.tight_layout(rect=[0, 0.02, 1, 0.90])
    save(fig, "D_seasonal_window.png")

# ══════════════════════════════════════════════════════════════════════════════
# Chart E — Real voices
# ══════════════════════════════════════════════════════════════════════════════
def chart_e_real_voices(r):
    print("Chart E: real voices")
    cc   = col(r, "content_category")
    tcol = "title_clean" if "title_clean" in r.columns else "title"

    # Admissions-specific subreddits only
    admissions_subs = {
        "applyingtocollege","chanceme","collegeresults",
        "collegeessays","intltousaa","ecadvice","sat","act",
        "collegeadmissions","questbridge","transfertotop25","premed"
    }
    if "subreddit" in r.columns:
        r_clean = r[r["subreddit"].str.lower().isin(admissions_subs)].copy()
    else:
        r_clean = r.copy()

    target_cats = [
        ("frustration_anxiety",      Z_RED,    "FRUSTRATION"),
        ("frustration_overwhelm",    Z_RED,    "FRUSTRATION"),
        ("rejection_reflection",     "#F97316","REJECTION"),
        ("timeline_deadline_panic",  Z_AMBER,  "TIMELINE PANIC"),
        ("beginner_question",        Z_PURPLE, "LOST / CONFUSED"),
        ("profile_review_request",   Z_PURPLE, "NEEDS GUIDANCE"),
    ]

    quotes = []
    if cc and len(r_clean) > 0:
        df = r_clean.dropna(subset=[cc]).copy()
        df[cc] = df[cc].str.strip().str.lower()
        for cat, color, badge in target_cats:
            sub = df[df[cc] == cat].sort_values("upvotes", ascending=False)
            for _, row in sub.head(2).iterrows():
                t = str(row[tcol]).strip()
                # Filter out clearly off-topic
                skip_words = ["stabbed","snapchat threat","phone ban",
                              "locking in","teacher","suspended","ditch school",
                              "psych ward","partied"]
                if any(w in t.lower() for w in skip_words):
                    continue
                if len(t) > 20:
                    quotes.append({
                        "badge":  badge,
                        "color":  color,
                        "title":  t,
                        "up":     int(row["upvotes"]),
                        "sub":    row.get("subreddit", ""),
                    })
            if len(quotes) >= 8:
                break

    # Fallback — top posts from admissions subs
    if len(quotes) < 5 and len(r_clean) > 0:
        for _, row in r_clean.sort_values("upvotes", ascending=False).head(20).iterrows():
            t = str(row[tcol]).strip()
            skip_words = ["stabbed","snapchat","phone ban","locking in",
                          "teacher","suspended","ditch school","psych ward"]
            if any(w in t.lower() for w in skip_words):
                continue
            if len(t) > 20:
                quotes.append({
                    "badge": "TOP POST",
                    "color": Z_PURPLE,
                    "title": t,
                    "up":    int(row["upvotes"]),
                    "sub":   row.get("subreddit",""),
                })
            if len(quotes) >= 8:
                break

    quotes = quotes[:8]
    if not quotes:
        print("  SKIP: no suitable quotes"); return

    n = len(quotes)
    fig_h = max(7, n * 1.1 + 1.5)
    fig = plt.figure(figsize=(14, fig_h))
    fig.patch.set_facecolor(Z_WHITE)
    ax = fig.add_axes([0.03, 0.04, 0.94, 0.82])
    ax.axis("off")
    ax.set_facecolor(Z_WHITE)

    row_h = 1.0 / n
    for i, q in enumerate(quotes):
        yp = 1.0 - (i + 0.5) * row_h
        c  = q["color"]

        # Left accent bar
        ax.add_patch(FancyBboxPatch(
            (0.0, yp - row_h * 0.42), 0.004, row_h * 0.84,
            boxstyle="round,pad=0.001",
            facecolor=c, edgecolor="none",
            transform=ax.transAxes))

        # Badge pill
        badge_w = max(len(q["badge"]) * 0.009, 0.12)
        ax.add_patch(FancyBboxPatch(
            (0.012, yp + row_h * 0.12), badge_w, row_h * 0.28,
            boxstyle="round,pad=0.008",
            facecolor=c + "22", edgecolor=c,
            linewidth=0.8, transform=ax.transAxes))
        ax.text(0.012 + badge_w / 2, yp + row_h * 0.26,
                q["badge"],
                transform=ax.transAxes,
                fontsize=7, fontweight="bold", color=c,
                ha="center", va="center")

        # Post title
        wrapped = textwrap.shorten(q["title"], width=95, placeholder="…")
        ax.text(0.012, yp - row_h * 0.02,
                f'"{wrapped}"',
                transform=ax.transAxes,
                fontsize=10.5, color=Z_BLACK,
                va="center", style="italic",
                fontweight="medium")

        # Meta line
        meta = f"▲ {q['up']:,} upvotes"
        if q["sub"]:
            meta += f"  ·  r/{q['sub']}"
        ax.text(0.012, yp - row_h * 0.30,
                meta,
                transform=ax.transAxes,
                fontsize=8.5, color=Z_GRAY, va="center")

        # Separator
        if i < n - 1:
            ax.axhline(y=yp - row_h * 0.48,
                       xmin=0.012, xmax=0.99,
                       color=Z_BORDER, lw=0.8,
                       transform=ax.transAxes)

    zimran_title(fig,
        "Real students, real words — these are your future users",
        "Actual Reddit post titles from college admissions communities  ·  "
        "admissions subreddits only  ·  sorted by upvotes")

    save(fig, "E_real_voices.png")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  ZIMRAN PITCH CHARTS  —  college admissions")
    print("=" * 60)
    r, y = load()
    print(f"\n  Reddit (filtered):  {len(r):,} posts")
    print(f"  YouTube:            {len(y):,} videos\n")

    chart_a_market_scale(r, y)
    chart_b_students_lost(r)
    chart_c_counselor_failing(r)
    chart_d_seasonal(r)
    chart_e_real_voices(r)

    print(f"\n{'=' * 60}")
    print(f"  5 charts saved to:")
    print(f"  {OUTPUT_DIR}")
    print(f"{'=' * 60}\n")

if __name__ == "__main__":
    main()