import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os

TOP_N       = 10
COLOR_TOP1  = '#EF233C'
COLOR_BARS  = '#3A86FF'
COLOR_OTHER = '#8D99AE'
BG_COLOR    = '#F8F9FA'


def _fmt(n: float) -> str:
    return f"{int(round(n)):,}".replace(",", " ") + " $"


def generate_debt_graph(debt_data, user_name, temp_dir):
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
            '[Client]': f'Інші ({n_other} контрагентів)',
            '[Sum_$]':  others['[Sum_$]'].sum()
        }])
        plot_df = pd.concat([top, other_row], ignore_index=True)
    else:
        plot_df = grouped.copy()
        n_other = 0

    # Reverse so largest appears at top in barh
    plot_df = plot_df.iloc[::-1].reset_index(drop=True)

    fig, ax = plt.subplots(
        figsize=(11, max(5, len(plot_df) * 0.58 + 1.5)),
        facecolor=BG_COLOR
    )
    ax.set_facecolor(BG_COLOR)

    top1_name = grouped.iloc[0]['[Client]']
    colors = []
    for lbl in plot_df['[Client]']:
        if lbl.startswith('Інші'):
            colors.append(COLOR_OTHER)
        elif lbl == top1_name:
            colors.append(COLOR_TOP1)
        else:
            colors.append(COLOR_BARS)

    bars = ax.barh(plot_df['[Client]'], plot_df['[Sum_$]'],
                   color=colors, height=0.6, edgecolor='none')

    max_val = plot_df['[Sum_$]'].max()
    for bar, val in zip(bars, plot_df['[Sum_$]']):
        pct   = val / total * 100
        label = f"  {_fmt(val)}  ·  {pct:.1f}%"
        ax.text(
            bar.get_width() + max_val * 0.01,
            bar.get_y() + bar.get_height() / 2,
            label, va='center', ha='left', fontsize=8.5, color='#495057'
        )

    ax.set_xlim(0, max_val * 1.48)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{int(x):,}".replace(",", " "))
    )
    ax.tick_params(axis='x', labelsize=8, colors='#6C757D')
    ax.tick_params(axis='y', labelsize=9)
    for spine in ('top', 'right', 'left'):
        ax.spines[spine].set_visible(False)
    ax.grid(axis='x', linestyle='--', alpha=0.4, color='#CED4DA')
    ax.set_axisbelow(True)

    ax.set_title(
        f"Дебіторка · {user_name} · {_fmt(total)}",
        fontsize=13, fontweight='bold', color='#212529', pad=14
    )
    if n_other:
        ax.set_xlabel(
            f"Топ-{TOP_N} контрагентів, решта об'єднані в «Інші»",
            fontsize=8, color='#6C757D'
        )

    plt.tight_layout()
    file_path = os.path.join(temp_dir, 'debt_graph.png')
    plt.savefig(file_path, dpi=130, facecolor=BG_COLOR, bbox_inches='tight')
    plt.close()
    return file_path
