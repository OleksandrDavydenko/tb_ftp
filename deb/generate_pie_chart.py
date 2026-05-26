import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os

TOP_N    = 10
BG_COLOR = '#F8F9FA'
PALETTE  = [
    '#3A86FF','#FF9F1C','#06A77D','#EF233C','#8338EC',
    '#FB5607','#2EC4B6','#E9C46A','#264653','#A8DADC',
    '#8D99AE',  # "Інші" — last, grey
]


def _fmt(n: float) -> str:
    return f"{int(round(n)):,}".replace(",", " ") + " $"


def generate_pie_chart(debt_data, user_name, temp_dir):
    df = pd.DataFrame(debt_data)
    if df.empty or '[Client]' not in df.columns or '[Sum_$]' not in df.columns:
        return None

    df['[Sum_$]'] = pd.to_numeric(df['[Sum_$]'], errors='coerce').fillna(0)
    grouped = df.groupby('[Client]', as_index=False)['[Sum_$]'].sum()
    grouped = grouped.sort_values('[Sum_$]', ascending=False).reset_index(drop=True)

    total = grouped['[Sum_$]'].sum()
    if total == 0:
        return None

    # Top-N + "Інші"
    if len(grouped) > TOP_N:
        top     = grouped.iloc[:TOP_N].copy()
        others  = grouped.iloc[TOP_N:]
        n_other = len(others)
        other_row = pd.DataFrame([{
            '[Client]': f'Інші ({n_other})',
            '[Sum_$]':  others['[Sum_$]'].sum()
        }])
        plot_df = pd.concat([top, other_row], ignore_index=True)
    else:
        plot_df = grouped.copy()

    values  = plot_df['[Sum_$]'].tolist()
    labels  = plot_df['[Client]'].tolist()
    colors  = PALETTE[:len(values)]

    # Labels inside wedge only for slices ≥ 3%
    def autopct(pct):
        return f'{pct:.1f}%' if pct >= 3 else ''

    fig, ax = plt.subplots(figsize=(11, 7), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    wedges, texts, autotexts = ax.pie(
        values,
        labels=None,
        autopct=autopct,
        colors=colors,
        startangle=90,
        wedgeprops=dict(width=0.52, edgecolor='white', linewidth=1.5),
        pctdistance=0.78,
    )
    for t in autotexts:
        t.set_fontsize(8)
        t.set_color('#212529')

    # Centre text
    ax.text(0, 0, f"{_fmt(total)}\n{len(grouped)} контраг.",
            ha='center', va='center', fontsize=10,
            fontweight='bold', color='#212529')

    # Legend on the right
    legend_patches = [
        mpatches.Patch(color=colors[i], label=f"{labels[i]}  —  {_fmt(values[i])}")
        for i in range(len(labels))
    ]
    ax.legend(
        handles=legend_patches,
        loc='center left',
        bbox_to_anchor=(1.01, 0.5),
        fontsize=8.5,
        frameon=False,
    )

    ax.set_title(
        f"Дебіторка · {user_name}",
        fontsize=13, fontweight='bold', color='#212529', pad=14
    )

    plt.tight_layout()
    file_path = os.path.join(temp_dir, 'debt_pie_chart.png')
    plt.savefig(file_path, dpi=130, facecolor=BG_COLOR, bbox_inches='tight')
    plt.close()
    return file_path
