"""
demand_validation.py  —  v2

Market validation charts for a personalized college admissions counseling product.
Reads from stage1/output/clean/reddit_enriched.csv (and youtube_enriched.csv).

Charts generated:
  01  Problem scale          — post volume over time, coloured by sentiment
  02  Demand vs supply gap   — content volume vs engagement inversion
  03  Competitor frustration — sentiment on posts mentioning existing tools
  04  Customer segments      — audience breakdown by engagement
  05  What to build          — pain-point keywords from high-engagement posts
  06  Seasonal anxiety       — month-of-year heatmap showing when anxiety peaks
  07  Pain point frequency   — how often each pain point appears
  08  Unanswered questions   — high-discussion, low-upvote posts
  09  Wish I knew            — retrospective advice posts from admitted students
  10  Real voices            — verbatim post titles showing the problem

Usage:
    cd C:\\Users\\Admin\\VGM
    py lab/stage2/pitch_charts/demand_validation.py
"""

import os, sys, json, textwrap
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
LAB_DIR     = os.path.dirname(os.path.dirname(SCRIPT_DIR))
CLEAN_DIR   = os.path.join(LAB_DIR, "stage1", "output", "clean")
CONFIG_PATH = os.path.join(LAB_DIR, "topic_config.json")
OUTPUT_DIR  = os.path.join(SCRIPT_DIR, "charts")
os.makedirs(OUTPUT_DIR, exist_ok=True)

REDDIT_PATH  = os.path.join(CLEAN_DIR, "reddit_enriched.csv")
YOUTUBE_PATH = os.path.join(CLEAN_DIR, "youtube_enriched.csv")

# ── Colours ───────────────────────────────────────────────────────────────────
BG     = "#0d1117"
SURF   = "#161b22"
BORDER = "#30363d"
T_PRI  = "#e6edf3"
T_SEC  = "#8b949e"
GREEN  = "#3fb950"
RED    = "#f85149"
BLUE   = "#58a6ff"
AMBER  = "#d29922"
PURPLE = "#bc8cff"
TEAL   = "#39d353"
CORAL  = "#ff7b72"

plt.rcParams.update({
    "figure.facecolor": BG,   "axes.facecolor":    SURF,
    "axes.edgecolor":   BORDER,"axes.labelcolor":  T_PRI,
    "axes.titlecolor":  T_PRI, "xtick.color":      T_SEC,
    "ytick.color":      T_SEC, "text.color":       T_PRI,
    "grid.color":       BORDER,"grid.alpha":       0.4,
    "font.family":      "monospace",
    "axes.spines.top":  False, "axes.spines.right":False,
    "axes.spines.left": False, "axes.spines.bottom":False,
})
MONTHS = ["Jan","Feb","Mar","Apr","May","Jun",
          "Jul","Aug","Sep","Oct","Nov","Dec"]

# ── Helpers ───────────────────────────────────────────────────────────────────
def col(df, bare):
    for name in (f"ai_llm_{bare}", bare):
        if name in df.columns:
            return name
    return None

def title_style(ax, title, sub=None):
    ax.set_title(title, fontsize=13, fontweight="bold",
                 color=T_PRI, pad=12, loc="left")
    if sub:
        ax.text(0, 1.015, sub, transform=ax.transAxes,
                fontsize=8, color=T_SEC, va="bottom")

def save(fig, name):
    p = os.path.join(OUTPUT_DIR, name)
    fig.savefig(p, dpi=150, bbox_inches="tight",
                facecolor=BG, edgecolor="none")
    plt.close(fig)
    print(f"  Saved: {name}")

def cfg_competitors():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            c = json.load(f)
        return [x.lower() for x in c.get("nlp",{}).get("competitors",[])]
    return ["collegevine","naviance","common app","collegeboard",
            "crimson","ivywise","counselor","consultant",
            "coalition app","scoir","prepory","cialfo","niche","bigfuture"]

def cfg_pain():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            c = json.load(f)
        return [x.lower() for x in c.get("nlp",{}).get("keywords_of_interest",[])]
    return ["stressed","overwhelmed","confused","too expensive",
            "can't afford","don't know where to start","wish i had",
            "running out of time","no idea","lost","waste of money",
            "too late","rejected","denied"]

def load():
    if not os.path.exists(REDDIT_PATH):
        print(f"ERROR: {REDDIT_PATH}"); sys.exit(1)
    r = pd.read_csv(REDDIT_PATH,  parse_dates=["date"])
    y = pd.read_csv(YOUTUBE_PATH, parse_dates=["date"]) \
        if os.path.exists(YOUTUBE_PATH) else pd.DataFrame()
    return r, y

# ── Chart 01 ──────────────────────────────────────────────────────────────────
def c01_problem_scale(r):
    print("Chart 01: problem scale")
    df = r.copy()
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    if "sentiment_label" in df.columns:
        df["s"] = df["sentiment_label"].str.lower()
    elif "sentiment_compound" in df.columns:
        df["s"] = df["sentiment_compound"].apply(
            lambda x: "positive" if x > 0.05 else ("negative" if x < -0.05 else "neutral"))
    else:
        df["s"] = "neutral"

    m = df.groupby(["month","s"]).size().unstack(fill_value=0).sort_index()
    for s in ["positive","neutral","negative"]:
        if s not in m.columns: m[s] = 0

    fig, ax = plt.subplots(figsize=(13,5))
    bot = np.zeros(len(m))
    for s, c, lb in [("positive",GREEN,"Positive"),
                     ("neutral", BLUE, "Neutral"),
                     ("negative",RED,  "Negative / anxious")]:
        v = m[s].values
        ax.bar(m.index, v, bottom=bot, color=c, alpha=0.82, width=20, label=lb)
        bot += v
    tot = m.sum(axis=1)
    z   = np.polyfit(np.arange(len(tot)), tot.values, 1)
    ax.plot(m.index, np.poly1d(z)(np.arange(len(tot))),
            color=AMBER, lw=2, ls="--", label="Growth trend", zorder=5)
    ax.set_xlabel("Month",fontsize=10); ax.set_ylabel("Posts",fontsize=10)
    ax.legend(loc="upper left",framealpha=0,fontsize=9)
    ax.yaxis.grid(True,alpha=0.3); ax.set_axisbelow(True)
    title_style(ax,
        "The college admissions conversation is growing — and it's anxious",
        f"Reddit post volume by sentiment  ·  {len(df):,} posts  ·  "
        f"{df['date'].dt.year.min()}–{df['date'].dt.year.max()}")
    fig.tight_layout(); save(fig,"01_problem_scale.png")

# ── Chart 02 ──────────────────────────────────────────────────────────────────
def c02_demand_gap(r):
    print("Chart 02: demand vs supply gap")
    cc = col(r,"content_category")
    if not cc: print("  SKIP"); return
    df = r.dropna(subset=[cc]).copy()
    df[cc] = df[cc].str.strip().str.lower()
    st = (df.groupby(cc)
            .agg(count=("upvotes","count"), avg=("upvotes","mean"))
            .reset_index())
    st = st[st["count"]>=5].sort_values("avg",ascending=True)
    lmap = {
        "frustration_anxiety":"Frustration & anxiety",
        "frustration_overwhelm":"Frustration & overwhelm",
        "beginner_question":"Beginner questions",
        "rejection_reflection":"Rejection stories",
        "success_story_acceptance":"Acceptance stories",
        "service_comparison":"Service comparisons",
        "service_comparison_review":"Service comparisons",
        "timeline_deadline_panic":"Timeline panic",
        "essay_feedback":"Essay feedback",
        "essay_feedback_request":"Essay feedback",
        "profile_review_request":"Profile reviews",
        "chance_evaluation_request":"Chance evaluation",
        "cost_affordability_complaint":"Cost complaints",
        "extracurricular_advice":"Extracurricular advice",
    }
    st["label"] = st[cc].map(lmap).fillna(
        st[cc].str.replace("_"," ").str.title())

    top3e = set(st.nlargest(3,"avg")[cc])
    bot3v = set(st.nsmallest(3,"count")[cc])
    under = top3e & bot3v

    fig, axes = plt.subplots(1,2,figsize=(15,6),sharey=True)
    fig.patch.set_facecolor(BG)
    for _, row in st.iterrows():
        u  = row[cc] in under
        axes[0].barh(row["label"], row["count"],
                     color=RED if u else BLUE, alpha=0.85, height=0.6)
        axes[1].barh(row["label"], row["avg"],
                     color=GREEN if u else AMBER, alpha=0.85, height=0.6)
    for ax,xl,tl in [(axes[0],"Number of posts","Volume  (how much is discussed)"),
                     (axes[1],"Avg upvotes","Engagement  (how much people care)")]:
        ax.set_xlabel(xl,fontsize=10)
        ax.set_title(tl,fontsize=11,color=T_SEC,pad=8)
        ax.xaxis.grid(True,alpha=0.3); ax.set_axisbelow(True)
    for i,(_, row) in enumerate(st.iterrows()):
        axes[0].text(row["count"]+0.5, i, str(int(row["count"])),
                     va="center",fontsize=8,color=T_SEC)
        axes[1].text(row["avg"]+2,     i, f"{row['avg']:.0f}",
                     va="center",fontsize=8,color=T_SEC)
    lg = [mpatches.Patch(color=RED,  alpha=.85,label="Underserved (low supply)"),
          mpatches.Patch(color=GREEN,alpha=.85,label="Underserved (high demand)"),
          mpatches.Patch(color=BLUE, alpha=.85,label="Normal supply")]
    fig.legend(handles=lg,loc="lower center",ncol=3,
               framealpha=0,fontsize=9,bbox_to_anchor=(0.5,-0.04))
    fig.suptitle("The gap: what people want most, no one is providing",
                 fontsize=14,fontweight="bold",color=T_PRI,
                 x=0.02,ha="left",y=1.01)
    fig.text(0.02,0.985,
             "Highest-engagement categories with fewest posts = unmet demand",
             fontsize=9,color=T_SEC)
    fig.tight_layout(); save(fig,"02_demand_supply_gap.png")

# ── Chart 03 ──────────────────────────────────────────────────────────────────
def c03_competitors(r):
    print("Chart 03: competitor frustration")
    comps   = cfg_competitors()
    tcol    = "title_clean" if "title_clean" in r.columns else "title"
    rows = []
    for c in comps:
        mask = r[tcol].str.lower().str.contains(c,na=False,regex=False)
        if "competitors_mentioned" in r.columns:
            mask = mask | r["competitors_mentioned"].str.lower().str.contains(
                c,na=False,regex=False)
        sub = r[mask]
        if len(sub)<2: continue
        rows.append({
            "competitor": c.title(),
            "mentions":   len(sub),
            "avg_sent":   sub["sentiment_compound"].mean()
                          if "sentiment_compound" in sub.columns else 0,
            "avg_upvotes":sub["upvotes"].mean(),
        })
    if not rows: print("  SKIP"); return
    df = pd.DataFrame(rows).sort_values("avg_sent")
    fig,ax = plt.subplots(figsize=(12,max(4,len(df)*.55)))
    fig.patch.set_facecolor(BG)
    colors = [RED if s<-0.05 else (GREEN if s>0.05 else BLUE)
              for s in df["avg_sent"]]
    ax.barh(df["competitor"],df["avg_sent"],color=colors,alpha=0.85,height=0.6)
    ax.axvline(0,color=BORDER,lw=1)
    ax.set_xlabel("Average sentiment  (negative ← 0 → positive)",fontsize=10)
    ax.xaxis.grid(True,alpha=0.3); ax.set_axisbelow(True)
    for _,row in df.iterrows():
        s=row["avg_sent"]
        ax.text(s+(0.004 if s>=0 else -0.004),
                list(df["competitor"]).index(row["competitor"]),
                f"  {row['mentions']} mentions",
                va="center",ha="left" if s>=0 else "right",
                fontsize=8,color=T_SEC)
    title_style(ax,
        "Students are frustrated with every existing solution",
        "Sentiment of Reddit posts mentioning each tool  "
        "·  negative = frustration  ·  positive = satisfaction")
    fig.tight_layout(); save(fig,"03_competitor_frustration.png")

# ── Chart 04 ──────────────────────────────────────────────────────────────────
def c04_segments(r):
    print("Chart 04: customer segments")
    ac = col(r,"target_audience")
    if not ac: print("  SKIP"); return
    df = r.dropna(subset=[ac]).copy()
    df[ac] = df[ac].str.strip().str.lower()
    st = (df.groupby(ac)
            .agg(count=("upvotes","count"),avg=("upvotes","mean"))
            .reset_index())
    st = st[st["count"]>=5].sort_values("avg",ascending=False)
    lmap = {
        "high_school_juniors_seniors":"HS juniors & seniors",
        "anxious_parents":"Anxious parents",
        "competitive_applicants_top_schools":"Top-school applicants",
        "first_generation_college_students":"First-gen students",
        "recent_admits_sharing_experience":"Recent admits",
        "international_students":"International students",
        "competitive_ivy_applicants":"Ivy applicants",
        "first_generation_students":"First-gen students",
        "beginners":"Beginners","students":"Students",
    }
    st["label"] = st[ac].map(lmap).fillna(
        st[ac].str.replace("_"," ").str.title())
    fig,ax = plt.subplots(figsize=(12,5))
    x  = np.arange(len(st))
    cs = [PURPLE,BLUE,TEAL,AMBER,GREEN,RED,CORAL][:len(st)]
    ax.bar(x,st["avg"],width=0.55,color=cs,alpha=0.88)
    for i,(_,row) in enumerate(st.iterrows()):
        ax.scatter(i,row["avg"]+8,s=row["count"]*6,
                   color=cs[i],alpha=0.25,zorder=3)
        ax.text(i,-22,f"n={int(row['count'])}",
                ha="center",fontsize=8,color=T_SEC)
    ax.set_xticks(x)
    ax.set_xticklabels(st["label"],rotation=15,ha="right",fontsize=10)
    ax.set_ylabel("Avg upvotes per post",fontsize=10)
    ax.yaxis.grid(True,alpha=0.3); ax.set_axisbelow(True)
    title_style(ax,
        "Every audience segment is highly engaged — this is a broad market",
        "Avg engagement per segment  ·  bubble size = post count")
    fig.tight_layout(); save(fig,"04_customer_segments.png")

# ── Chart 05 ──────────────────────────────────────────────────────────────────
def c05_keywords(r):
    print("Chart 05: what to build")
    if "top_keywords" not in r.columns: print("  SKIP"); return
    from collections import Counter
    thr  = r["upvotes"].quantile(0.75)
    high = r[r["upvotes"]>=thr]
    low  = r[r["upvotes"]< r["upvotes"].quantile(0.25)]
    def ext(df):
        c=Counter()
        for v in df["top_keywords"].dropna():
            for kw in str(v).replace("|",",").split(","):
                kw=kw.strip().lower()
                if 3<=len(kw)<=25 and "|" not in kw:
                    c[kw]+=1
        return c
    hc=ext(high); lc=ext(low)
    th=max(sum(hc.values()),1); tl=max(sum(lc.values()),1)
    scores={kw:hc.get(kw,0)/th - lc.get(kw,0)/tl
            for kw in set(hc)|set(lc)}
    top=[(k,v) for k,v in sorted(scores.items(),
         key=lambda x:x[1],reverse=True)[:20]
         if v>0 and "|" not in k]
    if not top: print("  SKIP"); return
    kws,vals=zip(*top)
    fmap={"harvard":PURPLE,"stanford":PURPLE,"mit":PURPLE,"yale":PURPLE,
          "ivy":PURPLE,"dream":PURPLE,
          "rejection":RED,"rejected":RED,"denied":RED,"waitlist":RED,
          "essay":GREEN,"personal":GREEN,"statement":GREEN,"writing":GREEN,
          "gpa":BLUE,"sat":BLUE,"act":BLUE,"score":BLUE,
          "parents":AMBER,"family":AMBER,"cost":AMBER,"afford":AMBER,
          "timeline":TEAL,"deadline":TEAL,"apply":TEAL}
    colors=[next((c for k,c in fmap.items() if k in kw.lower()),BLUE)
            for kw in kws]
    fig,ax=plt.subplots(figsize=(13,7))
    ax.barh(np.arange(len(kws)),vals,color=colors,alpha=0.88,height=0.65)
    ax.set_yticks(np.arange(len(kws))); ax.set_yticklabels(kws,fontsize=11)
    ax.set_xlabel("Keyword frequency lift in high-engagement posts",fontsize=10)
    ax.xaxis.grid(True,alpha=0.3); ax.set_axisbelow(True)
    lg=[mpatches.Patch(color=PURPLE,alpha=.88,label="Target school / prestige"),
        mpatches.Patch(color=RED,   alpha=.88,label="Rejection / anxiety"),
        mpatches.Patch(color=GREEN, alpha=.88,label="Essay & statement"),
        mpatches.Patch(color=BLUE,  alpha=.88,label="Test scores & GPA"),
        mpatches.Patch(color=AMBER, alpha=.88,label="Cost & family"),
        mpatches.Patch(color=TEAL,  alpha=.88,label="Timeline & deadlines")]
    ax.legend(handles=lg,loc="lower right",framealpha=0.15,fontsize=9,
              title="Product features implied",title_fontsize=9)
    title_style(ax,"The data tells you exactly what to build",
        "Keywords most common in high-engagement vs low-engagement posts")
    fig.tight_layout(); save(fig,"05_what_to_build.png")

# ── Chart 06  NEW — Seasonal anxiety heatmap ──────────────────────────────────
def c06_seasonal(r):
    print("Chart 06: seasonal anxiety heatmap")
    df = r.copy()
    df["mon"] = df["date"].dt.month
    if "sentiment_compound" in df.columns:
        df["neg"] = (df["sentiment_compound"] < -0.05).astype(int)
    elif "sentiment_label" in df.columns:
        df["neg"] = (df["sentiment_label"].str.lower()=="negative").astype(int)
    else:
        df["neg"] = 0

    m = df.groupby("mon").agg(
        total=("upvotes","count"),
        neg_pct=("neg","mean"),
        avg_eng=("upvotes","mean"),
    ).reindex(range(1,13),fill_value=0)

    fig,axes = plt.subplots(2,1,figsize=(13,7),sharex=True)
    fig.patch.set_facecolor(BG)
    x = np.arange(12)

    bar_colors=[plt.cm.RdYlGn_r(p) for p in m["neg_pct"].values]
    axes[0].bar(x,m["total"].values,color=bar_colors,alpha=0.9,width=0.7)
    axes[0].set_ylabel("Post volume",fontsize=10)
    axes[0].yaxis.grid(True,alpha=0.3); axes[0].set_axisbelow(True)
    for i,(v,p) in enumerate(zip(m["total"].values, m["neg_pct"].values)):
        if v>0:
            axes[0].text(i,v+1,f"{p*100:.0f}%\nneg",
                         ha="center",fontsize=7,color=T_SEC)

    axes[1].bar(x,m["avg_eng"].values,color=BLUE,alpha=0.75,width=0.7)
    axes[1].set_ylabel("Avg upvotes",fontsize=10)
    axes[1].yaxis.grid(True,alpha=0.3); axes[1].set_axisbelow(True)
    axes[1].set_xticks(x); axes[1].set_xticklabels(MONTHS,fontsize=10)

    for ax in axes:
        ax.axvspan(7.5,11.5,alpha=0.07,color=RED,   label="App season (Aug–Dec)")
        ax.axvspan(1.5, 3.5,alpha=0.07,color=GREEN, label="Decision season (Feb–Apr)")
    axes[0].legend(loc="upper left",framealpha=0,fontsize=8)

    sm=plt.cm.ScalarMappable(cmap="RdYlGn_r",norm=plt.Normalize(0,1))
    sm.set_array([])
    cb=fig.colorbar(sm,ax=axes[0],fraction=0.015,pad=0.01)
    cb.set_label("% negative",fontsize=8,color=T_SEC)

    fig.suptitle(
        "Anxiety peaks exactly when students need help most",
        fontsize=13,fontweight="bold",color=T_PRI,x=0.02,ha="left",y=1.01)
    fig.text(0.02,0.985,
             "Monthly post volume & engagement aggregated across all years  "
             "·  red = high anxiety  ·  shaded = key application windows",
             fontsize=8,color=T_SEC)
    fig.tight_layout(); save(fig,"06_seasonal_anxiety.png")

# ── Chart 07  NEW — Pain point frequency ──────────────────────────────────────
def c07_pain_points(r):
    print("Chart 07: pain point frequency")
    tcol = "title_clean" if "title_clean" in r.columns else "title"
    kws = [
        "stressed", "overwhelmed", "confused", "expensive",
        "afford", "don't know", "no idea", "lost",
        "wasted", "too late", "rejected", "denied",
        "help", "advice", "where to start", "scared",
        "worried", "anxiety", "panic", "desperate"
    ]
    rows = []
    for kw in kws:
        mask = r[tcol].str.lower().str.contains(kw, na=False, regex=False)
        cnt = mask.sum()
        if cnt < 2:
            continue
        rows.append({"keyword": kw, "count": cnt,
                     "avg_upvotes": r.loc[mask, "upvotes"].mean()})
    if not rows: print("  SKIP"); return
    df=pd.DataFrame(rows).sort_values("count",ascending=True)
    fig,ax=plt.subplots(figsize=(12,max(5,len(df)*.55)))
    fig.patch.set_facecolor(BG)
    norm=plt.Normalize(df["avg_upvotes"].min(),df["avg_upvotes"].max())
    colors=[plt.cm.YlOrRd(norm(v)) for v in df["avg_upvotes"]]
    ax.barh(df["keyword"],df["count"],color=colors,alpha=0.9,height=0.65)
    ax.set_xlabel("Posts containing this phrase",fontsize=10)
    ax.xaxis.grid(True,alpha=0.3); ax.set_axisbelow(True)
    for i,(_,row) in enumerate(df.iterrows()):
        ax.text(row["count"]+0.3,i,
                f" {int(row['count'])} posts  ·  {row['avg_upvotes']:.0f} avg ▲",
                va="center",fontsize=8,color=T_SEC)
    sm=plt.cm.ScalarMappable(cmap="YlOrRd",norm=norm)
    sm.set_array([])
    cb=fig.colorbar(sm,ax=ax,fraction=0.015,pad=0.01)
    cb.set_label("Avg upvotes",fontsize=8,color=T_SEC)
    title_style(ax,"The pain points are real — and high stakes",
        "Frequency of pain-point phrases  ·  colour = avg engagement")
    fig.tight_layout(); save(fig,"07_pain_point_frequency.png")

# ── Chart 08  NEW — Unanswered questions ──────────────────────────────────────
def c08_unanswered(r):
    print("Chart 08: unanswered questions")
    df=r[r["comments"]>=15].copy()
    if df.empty: print("  SKIP"); return
    df["controversy"]=df["comments"]/(df["upvotes"].clip(lower=1))
    df=df.sort_values("controversy",ascending=False).head(20)
    tcol="title_clean" if "title_clean" in df.columns else "title"
    fig,ax=plt.subplots(figsize=(13,max(6,len(df)*.6)))
    fig.patch.set_facecolor(BG)
    y=np.arange(len(df))
    ax.barh(y,df["comments"].values,color=BLUE, alpha=0.7,height=0.55,label="Comments")
    ax.barh(y,df["upvotes"].values, color=GREEN,alpha=0.7,height=0.55,label="Upvotes")
    ax.set_yticks(y)
    ax.set_yticklabels(
        [textwrap.shorten(str(t),width=62,placeholder="…")
         for t in df[tcol]],fontsize=8)
    ax.set_xlabel("Count",fontsize=10)
    ax.legend(loc="lower right",framealpha=0,fontsize=9)
    ax.xaxis.grid(True,alpha=0.3); ax.set_axisbelow(True)
    title_style(ax,"Unresolved questions — lots of discussion, no consensus answer",
        "Posts with highest comment-to-upvote ratio  "
        "·  these are the problems your product solves")
    fig.tight_layout(); save(fig,"08_unanswered_questions.png")

# ── Chart 09  NEW — Wish I knew ───────────────────────────────────────────────
def c09_wish(r):
    print("Chart 09: wish I knew")
    tcol="title_clean" if "title_clean" in r.columns else "title"
    pats = [
        "wish i", "if i could", "advice", "what worked",
        "how i got", "what got me", "actually matters",
        "looking back", "no one told", "didn't know",
        "should have", "would have", "could have",
        "lessons", "tips", "what i learned",
        "got accepted", "got in", "admitted"
    ]
    mask=r[tcol].str.lower().apply(lambda t:any(p in str(t) for p in pats))
    df=r[mask].sort_values("upvotes",ascending=False).head(15)
    if df.empty: print("  SKIP"); return
    fig,ax=plt.subplots(figsize=(13,max(5,len(df)*.6)))
    fig.patch.set_facecolor(BG)
    y=np.arange(len(df))
    sent=df.get("sentiment_compound",pd.Series([0]*len(df),index=df.index)).fillna(0)
    colors=[GREEN if s>0.05 else (RED if s<-0.05 else BLUE) for s in sent]
    ax.barh(y,df["upvotes"].values,color=colors,alpha=0.85,height=0.6)
    ax.set_yticks(y)
    ax.set_yticklabels(
        [textwrap.shorten(str(t),width=65,placeholder="…")
         for t in df[tcol]],fontsize=8)
    ax.set_xlabel("Upvotes",fontsize=10)
    for i,v in enumerate(df["upvotes"].values):
        ax.text(v+1,i,str(int(v)),va="center",fontsize=8,color=T_SEC)
    ax.xaxis.grid(True,alpha=0.3); ax.set_axisbelow(True)
    title_style(ax,"What students wish they had known — your product's curriculum",
        "Retrospective posts from admitted students  "
        "·  each title is a lesson your app should teach")
    fig.tight_layout(); save(fig,"09_wish_i_knew.png")

# ── Chart 10  NEW — Real voices ───────────────────────────────────────────────
def c10_real_voices(r):
    print("Chart 10: real voices")
    cc  =col(r,"content_category")
    tcol="title_clean" if "title_clean" in r.columns else "title"
    target=["frustration_anxiety","frustration_overwhelm",
            "rejection_reflection","timeline_deadline_panic",
            "beginner_question","success_story_acceptance"]
    cmap={"Frustration Anxiety":RED,"Frustration Overwhelm":RED,
          "Rejection Reflection":CORAL,"Timeline Deadline Panic":AMBER,
          "Beginner Question":BLUE,"Success Story Acceptance":GREEN,
          "Top Post":PURPLE}
    quotes=[]
    if cc:
        df=r.dropna(subset=[cc]).copy()
        df[cc]=df[cc].str.strip().str.lower()
        for cat in target:
            sub=df[df[cc]==cat].sort_values("upvotes",ascending=False)
            for _,row in sub.head(2).iterrows():
                t=str(row[tcol]).strip()
                if len(t)>20:
                    quotes.append({"cat":cat.replace("_"," ").title(),
                                   "title":t,"up":int(row["upvotes"]),
                                   "sub":row.get("subreddit","")})
    if len(quotes)<6:
        for _,row in r.sort_values("upvotes",ascending=False).head(12).iterrows():
            t=str(row[tcol]).strip()
            if len(t)>20:
                quotes.append({"cat":"Top Post","title":t,
                                "up":int(row["upvotes"]),
                                "sub":row.get("subreddit","")})
    quotes=quotes[:10]
    if not quotes: print("  SKIP"); return

    fig,ax=plt.subplots(figsize=(13,len(quotes)*.95+1.2))
    fig.patch.set_facecolor(BG); ax.axis("off")

    for i,q in enumerate(quotes):
        yp=1-(i+0.5)/len(quotes)
        c=cmap.get(q["cat"],BLUE)
        ax.text(0.01,yp+0.03,q["cat"].upper(),
                transform=ax.transAxes,fontsize=7,fontweight="bold",
                color=c,va="center")
        ax.text(0.01,yp,
                f'"{textwrap.shorten(q["title"],width=88,placeholder="…")}"',
                transform=ax.transAxes,fontsize=10,color=T_PRI,
                va="center",style="italic")
        meta=f"▲ {q['up']:,} upvotes"
        if q["sub"]: meta+=f"  ·  r/{q['sub']}"
        ax.text(0.01,yp-0.03,meta,
                transform=ax.transAxes,fontsize=8,color=T_SEC,va="center")
        ax.plot([0.01, 0.99], [yp-0.06, yp-0.06],
                color=BORDER, lw=0.5, transform=ax.transAxes)

    ax.set_title("Real voices from the data — the problem in their own words",
                 fontsize=13,fontweight="bold",color=T_PRI,pad=12,loc="left")
    ax.text(0,1.005,
            "Actual Reddit post titles from college admissions communities  "
            "·  these are your future users",
            transform=ax.transAxes,fontsize=8,color=T_SEC,va="bottom")
    fig.tight_layout(); save(fig,"10_real_voices.png")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("="*60)
    print("  DEMAND VALIDATION v2  —  college admissions pitch")
    print("="*60)
    r,y=load()
    print(f"\n  Reddit:  {len(r):,} posts")
    print(f"  YouTube: {len(y):,} videos\n")
    c01_problem_scale(r)
    c02_demand_gap(r)
    c03_competitors(r)
    c04_segments(r)
    c05_keywords(r)
    c06_seasonal(r)
    c07_pain_points(r)
    c08_unanswered(r)
    c09_wish(r)
    c10_real_voices(r)
    print(f"\n{'='*60}")
    print(f"  10 charts saved to:\n  {OUTPUT_DIR}")
    print(f"{'='*60}\n")

if __name__=="__main__":
    main()