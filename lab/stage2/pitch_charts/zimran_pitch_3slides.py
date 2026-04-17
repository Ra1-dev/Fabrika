"""
zimran_pitch_3slides.py  —  complete fixed version

3 pitch slides for Zimran Business Cup judging criteria:
  Slide 01 — Market size & growth dynamics
  Slide 02 — Clarity of customer pain points
  Slide 03 — Competitive landscape

Usage:
    cd C:\\Users\\Admin\\VGM
    py lab/stage2/pitch_charts/zimran_pitch_3slides.py
"""

import os, sys, json, textwrap
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, Rectangle
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LAB_DIR    = os.path.dirname(os.path.dirname(SCRIPT_DIR))
CLEAN_DIR  = os.path.join(LAB_DIR, "stage1", "output", "clean")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "zimran")
os.makedirs(OUTPUT_DIR, exist_ok=True)

REDDIT_PATH  = os.path.join(CLEAN_DIR, "reddit_enriched.csv")
YOUTUBE_PATH = os.path.join(CLEAN_DIR, "youtube_enriched.csv")

# ── Zimran palette ────────────────────────────────────────────────────────────
ZP  = "#6B4EFF"
ZPD = "#4B32CC"
ZPL = "#EAE6FF"
ZPM = "#B8A9FF"
ZBK = "#0D0D0D"
ZGY = "#6B7280"
ZGL = "#F3F4F6"
ZW  = "#FFFFFF"
ZRD = "#EF4444"
ZGN = "#10B981"
ZAM = "#F59E0B"
ZBD = "#E5E7EB"

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun",
          "Jul","Aug","Sep","Oct","Nov","Dec"]

# ── Helpers ───────────────────────────────────────────────────────────────────
def reset_style():
    plt.rcParams.update({
        "figure.facecolor": ZW,
        "axes.facecolor":   ZW,
        "axes.edgecolor":   ZBD,
        "axes.labelcolor":  ZGY,
        "axes.titlecolor":  ZBK,
        "xtick.color":      ZGY,
        "ytick.color":      ZGY,
        "text.color":       ZBK,
        "grid.color":       ZBD,
        "grid.alpha":       1.0,
        "font.family":      "DejaVu Sans",
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "axes.spines.left":  False,
        "axes.spines.bottom":True,
        "axes.linewidth":    0.8,
    })

def col(df, bare):
    for n in (f"ai_llm_{bare}", bare):
        if n in df.columns: return n
    return None

def save(fig, name):
    p = os.path.join(OUTPUT_DIR, name)
    fig.savefig(p, dpi=150, bbox_inches="tight",
                facecolor=ZW, edgecolor="none",
                pad_inches=0.1)
    plt.close(fig)
    print(f"  Saved: {name}")

def kpi_card(ax, x, y, w, h, value, label,
             color=ZP, bg=ZPL, val_size=28, lbl_size=8):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.015",
        facecolor=bg, edgecolor=ZBD, linewidth=0.8,
        transform=ax.transAxes, zorder=3))
    ax.text(x + w/2, y + h*0.63, value,
            transform=ax.transAxes,
            fontsize=val_size, fontweight="bold", color=color,
            ha="center", va="center", zorder=4)
    ax.text(x + w/2, y + h*0.22, label,
            transform=ax.transAxes,
            fontsize=lbl_size, color=ZGY,
            ha="center", va="center", zorder=4)

def draw_header(fig, num, criterion, title, subtitle):
    """Draw slide header using figure text — fixed y positions."""
    fig.text(0.04, 0.955,
             f"  {num:02d}  {criterion}  ",
             fontsize=9, fontweight="bold", color=ZW,
             va="center", ha="left",
             bbox=dict(facecolor=ZP, edgecolor="none",
                       boxstyle="round,pad=0.35"))
    fig.text(0.04, 0.910, title,
             fontsize=20, fontweight="bold", color=ZBK,
             va="center", ha="left")
    fig.text(0.04, 0.875, subtitle,
             fontsize=9.5, color=ZGY, va="center", ha="left")
    fig.add_artist(plt.Line2D([0.04, 0.96], [0.855, 0.855],
                              transform=fig.transFigure,
                              color=ZBD, lw=1.0))

def load():
    if not os.path.exists(REDDIT_PATH):
        print(f"ERROR: {REDDIT_PATH}"); sys.exit(1)
    r = pd.read_csv(REDDIT_PATH,  parse_dates=["date"])
    y = pd.read_csv(YOUTUBE_PATH, parse_dates=["date"]) \
        if os.path.exists(YOUTUBE_PATH) else pd.DataFrame()
    if "subreddit" in r.columns:
        r = r[~r["subreddit"].str.lower().isin(
            ["highschool", "a2c_circlejerk"])]
    return r, y

# ══════════════════════════════════════════════════════════════════════════════
# SLIDE 01 — Market size & growth dynamics
# ══════════════════════════════════════════════════════════════════════════════
def slide_01(r, y):
    print("Slide 01: market size")
    reset_style()

    df = r.copy()
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby("month").size().sort_index()

    early = monthly[monthly.index.year <= 2023].mean()
    late  = monthly[monthly.index.year >= 2026].mean()
    growth_x = int(round(late / max(early, 1)))

    fig = plt.figure(figsize=(16, 9), facecolor=ZW)

    draw_header(fig, 1, "MARKET SIZE & GROWTH DYNAMICS",
                "A $40B market — growing, underserved, and going digital",
                f"US college admissions counseling + test prep TAM  ·  "
                f"{len(r):,} Reddit posts + {len(y):,} YouTube videos analysed  ·  2023–2026")

    # content area: y from 0.08 to 0.84
    BOTTOM = 0.08
    HEIGHT = 0.74

    # ── Left funnel ───────────────────────────────────────────────────────────
    ax_f = fig.add_axes([0.04, BOTTOM, 0.26, HEIGHT])
    ax_f.set_xlim(0, 1); ax_f.set_ylim(0, 1)
    ax_f.axis("off")
    ax_f.set_facecolor(ZW)

    funnel = [
        ("TAM",  "$40B",  "Global edtech counseling\n+ test prep market",
         ZP,   0.90, 0.66),
        ("SAM",  "$1.75B","3.5M US seniors/yr\n× $500 willingness to pay",
         ZPD,  0.62, 0.38),
        ("SOM",  "$42M",  "Year 3 ARR target\n(1% share @ $20/mo)",
         ZPM,  0.34, 0.10),
    ]
    for label, value, desc, color, y_top, y_bot in funnel:
        w_top = y_top - 0.10
        w_bot = y_bot + 0.02
        xs = [0.50 - w_top/2, 0.50 + w_top/2,
              0.50 + w_bot/2, 0.50 - w_bot/2]
        ys = [y_top, y_top, y_bot, y_bot]
        ax_f.fill(xs, ys, color=color, alpha=0.88,
                  transform=ax_f.transAxes, zorder=3)
        mid = (y_top + y_bot) / 2
        ax_f.text(0.50, mid + 0.04, value,
                  transform=ax_f.transAxes,
                  fontsize=17, fontweight="bold", color=ZW,
                  ha="center", va="center", zorder=5)
        ax_f.text(0.50, mid - 0.04, label,
                  transform=ax_f.transAxes,
                  fontsize=8, color=ZW, alpha=0.85,
                  ha="center", va="center", zorder=5)
        ax_f.text(0.96, mid, desc,
                  transform=ax_f.transAxes,
                  fontsize=7.5, color=ZGY,
                  ha="left", va="center", zorder=5)

    ax_f.set_title("TAM / SAM / SOM",
                   fontsize=11, fontweight="bold",
                   color=ZBK, pad=8, loc="left")

    # ── Centre growth chart ───────────────────────────────────────────────────
    ax_g = fig.add_axes([0.34, BOTTOM, 0.40, HEIGHT])
    ax_g.set_facecolor(ZW)

    x = np.arange(len(monthly))
    ax_g.fill_between(x, 0, monthly.values, color=ZPL, alpha=0.7, zorder=1)
    ax_g.plot(x, monthly.values, color=ZP, lw=2.5, zorder=3)

    z = np.polyfit(x, monthly.values, 1)
    ax_g.plot(x, np.poly1d(z)(x), color=ZAM, lw=1.8,
              ls="--", alpha=0.85, label="Growth trend", zorder=4)

    months_idx = list(monthly.index)
    for i, m in enumerate(months_idx):
        if m.month in [9, 10, 11, 12]:
            ax_g.axvspan(i-0.5, i+0.5, alpha=0.07, color=ZP, zorder=0)

    tick_pos = [i for i, d in enumerate(months_idx) if d.month in (1, 7)]
    tick_lbl = [d.strftime("%b '%y") for d in months_idx if d.month in (1, 7)]
    ax_g.set_xticks(tick_pos)
    ax_g.set_xticklabels(tick_lbl, fontsize=8, color=ZGY)
    ax_g.set_ylabel("Posts / month", fontsize=9, color=ZGY)
    ax_g.yaxis.grid(True, color=ZBD, lw=0.8)
    ax_g.set_axisbelow(True)
    ax_g.spines["bottom"].set_color(ZBD)
    ax_g.legend(fontsize=8, framealpha=0, loc="upper left")

    peak_i = int(np.argmax(monthly.values))
    ax_g.annotate(f"Peak: {int(monthly.values[peak_i])} posts",
                  xy=(peak_i, monthly.values[peak_i]),
                  xytext=(peak_i - 8, monthly.values[peak_i] * 0.78),
                  fontsize=8, color=ZP,
                  arrowprops=dict(arrowstyle="->", color=ZP, lw=1.2))

    ax_g.set_title("Public discourse 2023 → 2026",
                   fontsize=11, fontweight="bold",
                   color=ZBK, pad=8, loc="left")

    # ── Right KPI cards ───────────────────────────────────────────────────────
    ax_k = fig.add_axes([0.78, BOTTOM, 0.19, HEIGHT])
    ax_k.axis("off")
    ax_k.set_facecolor(ZW)

    kpis = [
        (f"{growth_x}x",     "post volume growth\n2023 → 2026",        ZP,  ZPL),
        ("376",               "students per\nschool counselor",          ZRD, "#FEE2E2"),
        ("26%",               "high-achievers use\nprivate counselors",  ZPD, ZPL),
        (f"{len(r)+len(y):,}","data points\nanalysed",                  ZGY, ZGL),
    ]
    card_h = 0.19
    for i, (val, lbl, vc, bg) in enumerate(kpis):
        yp = 0.97 - i * (card_h + 0.02) - card_h
        kpi_card(ax_k, 0.02, yp, 0.96, card_h,
                 val, lbl, color=vc, bg=bg,
                 val_size=24, lbl_size=8)

    fig.text(0.04, 0.03,
             "Sources: IBISWorld 2023  ·  Navagant Education Report 2024  ·  "
             "ASCA counselor ratio  ·  Reddit public data 2023–2026",
             fontsize=7.5, color=ZGY, va="bottom")

    save(fig, "slide_01_market_size.png")


# ══════════════════════════════════════════════════════════════════════════════
# SLIDE 02 — Customer pain points
# ══════════════════════════════════════════════════════════════════════════════
def slide_02(r):
    print("Slide 02: pain points")
    reset_style()

    tcol = "title_clean" if "title_clean" in r.columns else "title"
    cc   = col(r, "content_category")

    adm_subs = {"applyingtocollege","chanceme","collegeresults",
                "collegeessays","intltousaa","ecadvice","sat","act",
                "collegeadmissions","questbridge","transfertotop25","premed"}
    if "subreddit" in r.columns:
        ra = r[r["subreddit"].str.lower().isin(adm_subs)].copy()
    else:
        ra = r.copy()

    # Seasonal data
    df = ra.copy()
    df["mon"] = df["date"].dt.month
    if "sentiment_compound" in df.columns:
        df["neg"] = (df["sentiment_compound"] < -0.05).astype(int)
    elif "sentiment_label" in df.columns:
        df["neg"] = (df["sentiment_label"].str.lower() == "negative").astype(int)
    else:
        df["neg"] = 0

    monthly_neg = df.groupby("mon").agg(
        total=("upvotes","count"),
        neg_pct=("neg","mean"),
    ).reindex(range(1,13), fill_value=0)

    # Category data
    cat_stats = None
    if cc and len(ra) > 0:
        dfc = ra.dropna(subset=[cc]).copy()
        dfc[cc] = dfc[cc].str.strip().str.lower()
        lmap = {
            "beginner_question":        "Beginner questions",
            "frustration_anxiety":      "Frustration & anxiety",
            "frustration_overwhelm":    "Frustration & overwhelm",
            "success_story_acceptance": "Acceptance stories",
            "rejection_reflection":     "Rejection stories",
            "timeline_deadline_panic":  "Timeline panic",
            "profile_review_request":   "Profile review requests",
            "service_comparison":       "Service comparisons",
            "essay_feedback":           "Essay feedback",
        }
        st = (dfc.groupby(cc)
                 .agg(count=("upvotes","count"),
                      avg=("upvotes","mean"))
                 .reset_index())
        st["label"] = st[cc].map(lmap).fillna(
            st[cc].str.replace("_"," ").str.title())
        cat_stats = st[st["count"] >= 15].sort_values("count", ascending=True)

    # Quotes
    skip = ["stabbed","snapchat","phone ban","locking in",
            "teacher","suspended","ditch school","psych ward",
            "partied","threat","bombing","fight","shooting"]
    quotes = []
    target = [
        ("beginner_question",       ZP,   "LOST / CONFUSED"),
        ("frustration_anxiety",     ZRD,  "FRUSTRATED"),
        ("frustration_overwhelm",   ZRD,  "OVERWHELMED"),
        ("rejection_reflection",    "#F97316", "REJECTED"),
        ("timeline_deadline_panic", ZAM,  "PANIC"),
        ("profile_review_request",  ZPD,  "NEEDS GUIDANCE"),
    ]
    if cc and len(ra) > 0:
        dfc2 = ra.dropna(subset=[cc]).copy()
        dfc2[cc] = dfc2[cc].str.strip().str.lower()
        for cat, color, badge in target:
            sub = dfc2[dfc2[cc] == cat].sort_values("upvotes", ascending=False)
            for _, row in sub.head(3).iterrows():
                t = str(row[tcol]).strip()
                if any(w in t.lower() for w in skip): continue
                if len(t) > 18:
                    quotes.append({"badge": badge, "color": color,
                                   "title": t,
                                   "up":    int(row["upvotes"]),
                                   "sub":   row.get("subreddit","")})
                if len(quotes) >= 6: break
            if len(quotes) >= 6: break

    # ── Build figure ──────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(16, 9), facecolor=ZW)

    draw_header(fig, 2, "CLARITY OF CUSTOMER PAIN POINTS",
                "623 frustration posts, 908 beginner questions — the problem is massive",
                "Based on 3,415 Reddit posts from college admissions communities  ·  "
                "admissions-specific subreddits only  ·  2023–2026")

    BOTTOM = 0.14
    HEIGHT = 0.68

    # ── Left: seasonal chart ──────────────────────────────────────────────────
    ax_s = fig.add_axes([0.04, BOTTOM, 0.27, HEIGHT])
    ax_s.set_facecolor(ZW)

    x = np.arange(12)
    max_t = max(monthly_neg["total"].values.max(), 1)
    for i, (tot, neg_p) in enumerate(zip(monthly_neg["total"].values,
                                          monthly_neg["neg_pct"].values)):
        c = ZP if i in [8, 9, 10, 11, 0, 1, 2] else ZPL
        alpha = 0.4 + 0.6 * (tot / max_t)
        ax_s.bar(i, tot, width=0.72, color=c,
                 alpha=min(alpha, 1.0), zorder=3,
                 edgecolor=ZW, linewidth=0.4)
        if tot > 3:
            ax_s.text(i, tot + max_t * 0.015,
                      f"{neg_p*100:.0f}%",
                      ha="center", fontsize=6.5, color=ZGY)

    ax_s.axvspan(7.5,  11.5, alpha=0.06, color=ZRD, zorder=0)
    ax_s.axvspan(0.5,   3.5, alpha=0.06, color=ZGN, zorder=0)
    ax_s.text(9.5, max_t * 0.88, "APP\nSEASON",
              ha="center", fontsize=7, fontweight="bold",
              color=ZRD, alpha=0.7)
    ax_s.text(2.0, max_t * 0.88, "DECISION\nSEASON",
              ha="center", fontsize=7, fontweight="bold",
              color=ZGN, alpha=0.7)
    ax_s.set_xticks(x)
    ax_s.set_xticklabels(MONTHS, fontsize=7.5)
    ax_s.set_ylabel("Posts / month", fontsize=9, color=ZGY)
    ax_s.yaxis.grid(True, color=ZBD, lw=0.8)
    ax_s.set_axisbelow(True)
    ax_s.spines["bottom"].set_color(ZBD)
    ax_s.set_title("Anxiety is seasonal — and predictable",
                   fontsize=11, fontweight="bold",
                   color=ZBK, pad=6, loc="left")

    # ── Centre: category bars ─────────────────────────────────────────────────
    ax_c = fig.add_axes([0.35, BOTTOM, 0.24, HEIGHT])
    ax_c.set_facecolor(ZW)

    if cat_stats is not None and len(cat_stats) > 0:
        focus = {"beginner questions", "frustration & anxiety",
                 "frustration & overwhelm", "rejection stories",
                 "timeline panic"}
        colors = [ZP if row["label"].lower() in focus else ZPL
                  for _, row in cat_stats.iterrows()]
        ax_c.barh(cat_stats["label"], cat_stats["count"],
                  color=colors, height=0.60, zorder=3)
        for i, (_, row) in enumerate(cat_stats.iterrows()):
            ax_c.text(row["count"] + 4, i,
                      f"{int(row['count'])}",
                      va="center", fontsize=8.5, color=ZGY)
        ax_c.set_xlabel("Number of posts", fontsize=9, color=ZGY)
        ax_c.xaxis.grid(True, color=ZBD, lw=0.8)
        ax_c.set_axisbelow(True)
        ax_c.spines["bottom"].set_color(ZBD)
        ax_c.spines["left"].set_visible(False)

    ax_c.set_title("What students post about",
                   fontsize=11, fontweight="bold",
                   color=ZBK, pad=6, loc="left")

    # ── Right: real voices ────────────────────────────────────────────────────
    ax_v = fig.add_axes([0.63, BOTTOM, 0.34, HEIGHT])
    ax_v.axis("off")
    ax_v.set_facecolor(ZW)
    ax_v.set_title("Real students, in their own words",
                   fontsize=11, fontweight="bold",
                   color=ZBK, pad=6, loc="left")

    nq = max(len(quotes), 1)
    rh = 0.88 / nq
    for i, q in enumerate(quotes[:6]):
        yp = 0.90 - i * rh
        c  = q["color"]

        ax_v.add_patch(FancyBboxPatch(
            (0.0, yp - rh*0.40), 0.005, rh*0.80,
            boxstyle="round,pad=0.001",
            facecolor=c, edgecolor="none",
            transform=ax_v.transAxes))

        bw = max(len(q["badge"]) * 0.011, 0.14)
        ax_v.add_patch(FancyBboxPatch(
            (0.014, yp + rh*0.10), bw, rh*0.32,
            boxstyle="round,pad=0.008",
            facecolor=c+"22", edgecolor=c,
            linewidth=0.8, transform=ax_v.transAxes))
        ax_v.text(0.014 + bw/2, yp + rh*0.26,
                  q["badge"],
                  transform=ax_v.transAxes,
                  fontsize=6.5, fontweight="bold", color=c,
                  ha="center", va="center")

        short = textwrap.shorten(q["title"], width=62, placeholder="…")
        ax_v.text(0.014, yp - rh*0.04,
                  f'"{short}"',
                  transform=ax_v.transAxes,
                  fontsize=9, color=ZBK,
                  va="center", style="italic")

        meta = f"▲ {q['up']:,}"
        if q["sub"]: meta += f"  r/{q['sub']}"
        ax_v.text(0.014, yp - rh*0.34,
                  meta, transform=ax_v.transAxes,
                  fontsize=7.5, color=ZGY, va="center")

        if i < nq - 1:
            ax_v.plot([0.014, 0.99],
                      [yp - rh*0.48, yp - rh*0.48],
                      color=ZBD, lw=0.7,
                      transform=ax_v.transAxes)

    # Bottom stat bar
    fig.add_artist(plt.Line2D([0.04, 0.96], [0.115, 0.115],
                              transform=fig.transFigure,
                              color=ZBD, lw=0.8))
    fig.text(0.50, 0.075,
             "376 students per school counselor  (recommended: 250)    ·    "
             "\"help\" in 130 posts    ·    "
             "\"rejected\" posts avg 324 upvotes    ·    "
             "Application season engagement 2× higher than off-season",
             fontsize=8.5, color=ZP, ha="center", fontweight="bold")
    fig.text(0.04, 0.030,
             "Source: Reddit public data 2023–2026  ·  ASCA counselor ratio  ·  "
             "admissions subreddits only",
             fontsize=7.5, color=ZGY)

    save(fig, "slide_02_pain_points.png")


# ══════════════════════════════════════════════════════════════════════════════
# SLIDE 03 — Competitive landscape
# ══════════════════════════════════════════════════════════════════════════════
def slide_03(r):
    print("Slide 03: competitive landscape")
    reset_style()

    tcol = "title_clean" if "title_clean" in r.columns else "title"

    comp_search = [
        ("collegevine",  "CollegeVine"),
        ("naviance",     "Naviance"),
        ("common app",   "Common App"),
        ("counselor",    "School counselor"),
        ("consultant",   "Private consultant"),
        ("crimson",      "Crimson Education"),
        ("collegeboard", "College Board"),
        ("khan academy", "Khan Academy"),
    ]
    comp_data = {}
    for kw, label in comp_search:
        mask = r[tcol].str.lower().str.contains(kw, na=False, regex=False)
        if "competitors_mentioned" in r.columns:
            mask = mask | r["competitors_mentioned"].str.lower().str.contains(
                kw, na=False, regex=False)
        sub = r[mask]
        if len(sub) >= 2:
            comp_data[label] = {
                "mentions":  len(sub),
                "sentiment": sub["sentiment_compound"].mean()
                             if "sentiment_compound" in sub.columns else 0,
                "avg_up":    sub["upvotes"].mean(),
            }

    fig = plt.figure(figsize=(16, 9), facecolor=ZW)

    draw_header(fig, 3, "COMPETITIVE LANDSCAPE",
                "Every existing solution fails on personalisation, affordability, or both",
                "Positioning analysis  ·  sentiment from Reddit mentions  ·  "
                "our product targets the empty quadrant")

    BOTTOM = 0.10
    HEIGHT = 0.72

    # ── Left: positioning matrix ──────────────────────────────────────────────
    ax_m = fig.add_axes([0.04, BOTTOM, 0.42, HEIGHT])
    ax_m.set_facecolor(ZW)
    ax_m.set_xlim(-0.05, 1.05)
    ax_m.set_ylim(-0.05, 1.05)

    ax_m.axvline(0.5, color=ZBD, lw=1.2, ls="--", zorder=1)
    ax_m.axhline(0.5, color=ZBD, lw=1.2, ls="--", zorder=1)

    # Highlight empty quadrant (affordable + personalised)
    ax_m.add_patch(Rectangle((0.50, 0.50), 0.55, 0.55,
                               facecolor=ZPL, edgecolor=ZP,
                               linewidth=1.5, linestyle="--",
                               zorder=0, alpha=0.35))

    # Quadrant labels
    for (tx, ty, txt, c, fw) in [
        (0.25, 1.02, "AFFORDABLE  ·  LOW PERSONALISATION",  ZGY, "normal"),
        (0.75, 1.02, "AFFORDABLE  ·  HIGH PERSONALISATION", ZP,  "bold"),
        (0.25,-0.04, "EXPENSIVE  ·  LOW PERSONALISATION",   ZGY, "normal"),
        (0.75,-0.04, "EXPENSIVE  ·  HIGH PERSONALISATION",  ZGY, "normal"),
    ]:
        ax_m.text(tx, ty, txt,
                  fontsize=7.5, color=c, fontweight=fw,
                  ha="center", va="center")

    # Competitor positions: (x=affordability 0=expensive,1=free, y=personalisation)
    positions = {
        "School counselor":   (0.68, 0.30),
        "CollegeVine":        (0.78, 0.28),
        "Common App":         (0.82, 0.10),
        "Khan Academy":       (0.84, 0.15),
        "Naviance":           (0.62, 0.22),
        "Private consultant": (0.08, 0.88),
        "Crimson Education":  (0.04, 0.82),
        "College Board":      (0.75, 0.12),
    }
    for label, (px, py) in positions.items():
        ax_m.scatter(px, py, s=80, color=ZGY, zorder=4,
                     edgecolors=ZW, linewidths=0.8)
        offset_x = 0.04
        ha = "left"
        if px > 0.78:
            offset_x = -0.04; ha = "right"
        ax_m.text(px + offset_x, py + 0.05, label,
                  fontsize=7.5, color=ZGY, ha=ha, va="bottom", zorder=5)

    # Our product star
    ax_m.scatter(0.80, 0.82, s=250, color=ZP, zorder=6,
                 marker="*", edgecolors=ZPD, linewidths=0.8)
    ax_m.text(0.80, 0.71, "OUR PRODUCT",
              fontsize=9.5, fontweight="bold", color=ZP,
              ha="center", va="top", zorder=7)
    ax_m.text(0.80, 0.65,
              "AI-powered · affordable\n· fully personalised",
              fontsize=7.5, color=ZPD, ha="center", va="top", zorder=7)

    ax_m.set_xlabel("← Expensive                                   Affordable →",
                    fontsize=9, color=ZGY, labelpad=6)
    ax_m.set_ylabel("← Generic                                   Personalised →",
                    fontsize=9, color=ZGY, labelpad=6)
    ax_m.set_xticks([]); ax_m.set_yticks([])
    ax_m.spines["bottom"].set_color(ZBD)
    ax_m.spines["left"].set_color(ZBD)
    ax_m.set_title("Market positioning map",
                   fontsize=11, fontweight="bold",
                   color=ZBK, pad=8, loc="left")

    # ── Top right: sentiment bars ─────────────────────────────────────────────
    ax_st = fig.add_axes([0.51, BOTTOM + 0.37, 0.46, 0.35])
    ax_st.set_facecolor(ZW)

    if comp_data:
        cd = (pd.DataFrame(comp_data).T
                .reset_index()
                .rename(columns={"index":"label"}))
        cd["sentiment"] = cd["sentiment"].astype(float)
        cd = cd.sort_values("sentiment")
        s_colors = [ZRD if s < -0.02 else (ZGN if s > 0.03 else ZGY)
                    for s in cd["sentiment"]]
        ax_st.barh(cd["label"], cd["sentiment"],
                   color=s_colors, height=0.55, zorder=3)
        ax_st.axvline(0, color=ZBD, lw=1.5, zorder=2)
        for i, (_, row) in enumerate(cd.iterrows()):
            s = float(row["sentiment"])
            ax_st.text(s + (0.003 if s >= 0 else -0.003), i,
                       f" {int(row['mentions'])} mentions",
                       va="center",
                       ha="left" if s >= 0 else "right",
                       fontsize=7.5, color=ZGY)
        ax_st.set_xlabel("Avg sentiment  (negative = frustration)",
                         fontsize=8, color=ZGY)
        ax_st.xaxis.grid(True, color=ZBD, lw=0.8)
        ax_st.set_axisbelow(True)
        ax_st.spines["bottom"].set_color(ZBD)
        ax_st.spines["left"].set_visible(False)

    ax_st.set_title("How students feel about existing tools",
                    fontsize=11, fontweight="bold",
                    color=ZBK, pad=6, loc="left")

    # ── Bottom right: advantage cards ─────────────────────────────────────────
    ax_adv = fig.add_axes([0.51, BOTTOM, 0.46, 0.33])
    ax_adv.axis("off")
    ax_adv.set_facecolor(ZW)
    ax_adv.set_title("Why we win",
                     fontsize=11, fontweight="bold",
                     color=ZBK, pad=6, loc="left")

    advantages = [
        ("$20/mo",  "vs $150–500/hr\nprivate counselor",     ZP,  ZPL),
        ("24 / 7",  "availability vs\noverloaded counselors", ZPD, ZPL),
        ("1 app",   "vs 4+ separate\ntools students juggle",  ZGY, ZGL),
    ]
    cw = 0.28
    for i, (val, lbl, vc, bg) in enumerate(advantages):
        xp = 0.04 + i * (cw + 0.04)
        kpi_card(ax_adv, xp, 0.12, cw, 0.72,
                 val, lbl, color=vc, bg=bg,
                 val_size=22, lbl_size=8)

    fig.text(0.04, 0.03,
             "Sources: competitor pricing from public websites  ·  "
             "Reddit sentiment from 3,415 posts  ·  ASCA counselor ratio",
             fontsize=7.5, color=ZGY)

    save(fig, "slide_03_competitive.png")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  ZIMRAN PITCH  —  3 slides")
    print("=" * 60)
    r, y = load()
    print(f"\n  Reddit (filtered):  {len(r):,} posts")
    print(f"  YouTube:            {len(y):,} videos\n")
    slide_01(r, y)
    slide_02(r)
    slide_03(r)
    print(f"\n{'=' * 60}")
    print(f"  Slides saved to:  {OUTPUT_DIR}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()