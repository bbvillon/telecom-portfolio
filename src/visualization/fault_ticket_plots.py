"""
Fault Ticket Domain-Specific Plots
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import sys
sys.path.insert(0, '../..')
from config import PROFESSIONAL_PALETTE, ZONE_PALETTE, ZONE_ORDER
from src.utils import sort_by_zone
from .common_plots import create_bar_graph
from src.fault_ticket.metrics import calculate_zone_summary


def plot_average_resolution_time(ax, df=None, summary=None):
    """Plot MTTR by zone."""
    if summary is None:
        summary = calculate_zone_summary(df)
    
    mttr_data = summary[['ZONE', 'MTTR']]
    mttr_data = sort_by_zone(mttr_data, 'ZONE')
    
    create_bar_graph(
        ax, mttr_data, 'ZONE', 'MTTR',
        title='Average Total MTTR per Zone',
        x_label='Zone', y_label='Hours',
        show_value=True, palette=PROFESSIONAL_PALETTE
    )


def plot_sla_compliance(ax, df=None, summary=None):
    """Plot SLA compliance by zone."""
    if summary is None:
        summary = calculate_zone_summary(df)
    
    sla_data = summary[['ZONE', 'SLA_Compliance_Rate']]
    sla_data = sort_by_zone(sla_data, 'ZONE')
    
    create_bar_graph(
        ax, sla_data, 'ZONE', 'SLA_Compliance_Rate',
        title='SLA Compliance by Zone',
        x_label='Zone', y_label='Compliance Rate (%)',
        show_value=True, palette=PROFESSIONAL_PALETTE
    )


def plot_ticket_volume(ax, df=None, summary=None):
    """Plot ticket volume by zone."""
    if summary is None:
        summary = calculate_zone_summary(df)
    
    ticket_data = summary[['ZONE', 'Ticket_Count']]
    ticket_data = sort_by_zone(ticket_data, 'ZONE')
    
    create_bar_graph(
        ax, ticket_data, 'ZONE', 'Ticket_Count',
        title='Ticket Volume per Zone',
        x_label='Zone', y_label='Count',
        show_value=True, palette=PROFESSIONAL_PALETTE
    )


def plot_ticket_volume_distribution(ax, df):
    """Pie chart of ticket distribution by zone. Unknown zone excluded."""
    df_known = df[df['ZONE'].isin(ZONE_ORDER)].copy()
    volume   = df_known.groupby('ZONE').size().reindex(ZONE_ORDER).reset_index(name='Ticket_Count')

    colors  = [ZONE_PALETTE.get(z, '#cccccc') for z in volume['ZONE']]
    explode = [0.08 if z == 'ZONE 5' else 0 for z in volume['ZONE']]

    wedges, texts, autotexts = ax.pie(
        volume['Ticket_Count'],
        labels=volume['ZONE'],
        autopct='%1.1f%%',
        startangle=90,
        colors=colors,
        explode=explode,
        pctdistance=0.78,
        labeldistance=1.12,
        wedgeprops={'linewidth': 0.8, 'edgecolor': 'white'}
    )
    for t in texts:     t.set_fontsize(8)
    for t in autotexts: t.set_fontsize(7.5); t.set_fontweight('bold')
    ax.set_title('Ticket Volume Distribution by Zone', fontsize=11, fontweight='bold', pad=8)

def plot_fault_density(ax, df=None, summary=None):
    """Plot fault density by zone."""
    if summary is None:
        summary = calculate_zone_summary(df)

    fault_density_data = summary[['ZONE', 'Fault_Density']]
    fault_density_data = sort_by_zone(fault_density_data, 'ZONE')

    create_bar_graph(
        ax, fault_density_data, 'ZONE', 'Fault_Density',
        title='Fault Density (Faults per Site)',
        x_label='Zone', y_label='Faults/Site',
        show_value=True, palette=PROFESSIONAL_PALETTE
    )

def plot_noc_vs_field_time(ax, df=None, summary=None):
    """
    Grouped bar chart: NOC/ROC Time vs Field Engineer Time per zone.

    NOC/ROC Time  = DISPATCHDATE - REPORTDATE  (remote troubleshooting phase)
    Field Time    = RESOLVEDDATE - DISPATCHDATE (on-site resolution phase)

    Replaces the old plot_average_dispatch_delay() which only showed NOC time
    and used a misleading label.
    """
    if summary is None:
        summary = calculate_zone_summary(df)

    # Ensure both columns exist
    if 'Avg_NOC_Time' not in summary.columns:
        summary = summary.rename(columns={'Avg_Dispatch_Delay': 'Avg_NOC_Time'})
    if 'Avg_Field_Time' not in summary.columns:
        summary['Avg_Field_Time'] = summary['MTTR'] - summary['Avg_NOC_Time']

    data = summary.set_index('ZONE').reindex(ZONE_ORDER).reset_index()
    x    = np.arange(len(ZONE_ORDER))
    w    = 0.38

    bn = ax.bar(x - w/2, data['Avg_NOC_Time'],   w,
                label='NOC/ROC Time',   color='#5b9bd5', edgecolor='white', linewidth=0.6)
    bf = ax.bar(x + w/2, data['Avg_Field_Time'], w,
                label='Field Engr Time', color='#ed7d31', edgecolor='white', linewidth=0.6)

    for b in list(bn) + list(bf):
        h = b.get_height()
        if h > 1:
            ax.text(b.get_x()+b.get_width()/2, h+0.8,
                    f'{h:.0f}h', ha='center', va='bottom', fontsize=6.5, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(ZONE_ORDER, fontsize=8)
    ax.legend(fontsize=8, framealpha=0.7, loc='upper right')
    ax.set_title('NOC/ROC Time vs Field Engineer Time', fontsize=11, fontweight='bold', pad=8)
    ax.set_ylabel('Hours', fontsize=9)
    ax.set_xlabel('Zone', fontsize=9)
    ax.grid(axis='y', alpha=0.25, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

def plot_average_dispatch_delay(ax, df=None, summary=None):
    """
    Deprecated. Use plot_noc_vs_field_time() for new code.
    Preserved for backward compatibility.
    """
    plot_noc_vs_field_time(ax, df=df, summary=summary)


def plot_grouped_bar_by_zone(ax, pivot_df, zone_order, col_order,
                             col_colors, col_labels,
                             ylabel, title,
                             fmt='{:.0f}h', annotate_threshold=2,
                             bar_width=0.26, alpha=0.88):
    """
    Grouped bar chart: one bar-group per zone, one bar per category (column).

    Used wherever a metric is broken down by both zone and a categorical split
    (resolution path, priority tier, NE category, etc.).

    Parameters
    ----------
    ax : matplotlib Axes
    pivot_df : DataFrame
        Index = ZONE_ORDER, columns = col_order. Values are the bar heights.
    zone_order : list of str
    col_order : list of str
        Columns from pivot_df to plot, in left-to-right order within each group.
    col_colors : list of str
        One hex colour per column (matches col_order).
    col_labels : list of str
        Legend labels (matches col_order).
    ylabel : str
    title : str
    fmt : str
        Format string for bar annotations, e.g. '{:.0f}h' or '{:.1f}%'.
    annotate_threshold : float
        Bars shorter than this value get no annotation (avoids clutter).
    bar_width : float
        Width of each individual bar.
    alpha : float

    Example — MTTR by resolution path
    -----------------------------------
    fig, ax = plt.subplots(figsize=(13, 5))
    fig.patch.set_facecolor('#fafafa')
    plot_grouped_bar_by_zone(
        ax, mttr_path, ZONE_ORDER,
        col_order   = ['Auto_Self_Restored', 'NOC_Remote_Restored', 'Field_Dispatch_Restored'],
        col_colors  = [PATH_COLORS[p] for p in paths],
        col_labels  = [PATH_LABELS[p] for p in paths],
        ylabel='Avg MTTR (hours)',
        title='Average MTTR by Resolution Path per Zone',
        fmt='{:.0f}h',
    )
    plt.tight_layout()
    plt.savefig('reports/figures/...', dpi=150, bbox_inches='tight')
    """
    x = np.arange(len(zone_order))
    n = len(col_order)
    # Centre the group: offset each bar symmetrically around 0
    offsets = [(i - (n - 1) / 2) * bar_width for i in range(n)]

    for col, color, label, offset in zip(col_order, col_colors, col_labels, offsets):
        vals = pivot_df[col].reindex(zone_order).values
        bars = ax.bar(x + offset, vals, bar_width,
                      label=label, color=color,
                      edgecolor='white', linewidth=0.5, alpha=alpha)
        for bar in bars:
            h = bar.get_height()
            if h > annotate_threshold:
                ax.text(bar.get_x() + bar.get_width() / 2, h + 0.5,
                        fmt.format(h),
                        ha='center', va='bottom', fontsize=7.5, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels(zone_order, fontsize=9)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_title(title, fontsize=11, fontweight='bold', pad=8)
    ax.legend(fontsize=9, framealpha=0.8)
    ax.grid(axis='y', alpha=0.25, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


def plot_dual_barh(ax_left, ax_right, data, label_col,
                   time_col, sla_col,
                   time_xlabel='Avg Time (hours)',
                   sla_xlabel='SLA Compliance (%)',
                   title_left='', title_right='',
                   palette=None, sla_target=90):
    """
    Side-by-side horizontal bar charts: a time metric (left) and SLA rate (right).

    Used for RFO and NE category breakdowns — any case where you want to show
    both how long something takes AND what its compliance rate is, on the same
    set of row labels.

    Parameters
    ----------
    ax_left, ax_right : matplotlib Axes
        The two axes to draw on — caller creates the figure.
    data : DataFrame
        Pre-aggregated, pre-sorted. Each row = one category (RFO, NE type, etc.)
    label_col : str
        Column with row labels (e.g. 'Standardized RFO', 'NE_Category').
    time_col : str
        Column for left panel (hours).
    sla_col : str
        Column for right panel (%). Should already be 0–100.
    palette : list of str, optional
        One colour per row. Defaults to PROFESSIONAL_PALETTE.
    sla_target : float
        Value at which to draw the target reference line on the right panel.

    Example
    -------
    fig, axes = plt.subplots(1, 2, figsize=(17, 6))
    fig.patch.set_facecolor('#fafafa')
    plot_dual_barh(
        axes[0], axes[1], rfo_field,
        label_col='Standardized RFO',
        time_col='Avg_Field_Time', sla_col='SLA_Rate',
        time_xlabel='Avg Field Time (hours)', sla_xlabel='SLA Compliance (%)',
        title_left='Avg Field Time by RFO\\n(Field Dispatch · clean timestamps)',
        title_right='SLA Rate by RFO Category',
        palette=[RFO_PALETTE.get(r, '#5b9bd5') for r in rfo_field['Standardized RFO']],
    )
    plt.tight_layout()
    plt.savefig('reports/figures/...', dpi=150, bbox_inches='tight')
    """
    if palette is None:
        palette = PROFESSIONAL_PALETTE[:len(data)]

    _spine_off = lambda ax: (ax.spines['top'].set_visible(False),
                              ax.spines['right'].set_visible(False))

    # ── Left: time metric ────────────────────────────────────────────
    bars = ax_left.barh(data[label_col], data[time_col],
                        color=palette, edgecolor='white', linewidth=0.5)
    for bar, val in zip(bars, data[time_col]):
        ax_left.text(bar.get_width() + 0.3,
                     bar.get_y() + bar.get_height() / 2,
                     f'{val:.0f}h', va='center', fontsize=8.5)
    ax_left.set_xlabel(time_xlabel, fontsize=10)
    ax_left.set_title(title_left, fontsize=11, fontweight='bold', pad=8)
    ax_left.invert_yaxis()
    ax_left.grid(axis='x', alpha=0.25, linestyle='--')
    _spine_off(ax_left)

    # ── Right: SLA rate ──────────────────────────────────────────────
    bars2 = ax_right.barh(data[label_col], data[sla_col],
                           color=palette, edgecolor='white', linewidth=0.5)
    ax_right.axvline(sla_target, color='#d62728', linestyle='--',
                     linewidth=1, label=f'Target {sla_target}%')
    for bar, val in zip(bars2, data[sla_col]):
        ax_right.text(bar.get_width() + 0.2,
                      bar.get_y() + bar.get_height() / 2,
                      f'{val:.1f}%', va='center', fontsize=8.5)
    ax_right.set_xlabel(sla_xlabel, fontsize=10)
    ax_right.set_title(title_right, fontsize=11, fontweight='bold', pad=8)
    ax_right.invert_yaxis()
    ax_right.legend(fontsize=9)
    ax_right.grid(axis='x', alpha=0.25, linestyle='--')
    _spine_off(ax_right)


def plot_city_metric_by_zone(city_df, zone_order, value_col,
                              title_tmpl, xlabel,
                              color_fn, ref_line_fn,
                              annotation_fn, annotation_extra_col=None,
                              top_n=15, sort_col=None,
                              figsize=(18, 11), save_path=None):
    """
    6-panel (2×3) horizontal bar chart: one subplot per zone, cities on y-axis.

    Used for any metric that needs to be shown per-city within each zone —
    SLA compliance, MTTR, fault density, P3.2 breach rate, endorsement delay.

    Parameters
    ----------
    city_df : DataFrame
        Must have 'ZONE' and 'CITY' columns plus value_col (and annotation_extra_col).
    zone_order : list of str
        Defines subplot order.
    value_col : str
        Column to plot as bar length.
    title_tmpl : str
        F-string template with '{zone}' placeholder,
        e.g. '{zone} — City SLA Compliance\\n(top 15 by volume)'.
    xlabel : str
        X-axis label.
    color_fn : callable(val, zone_avg) -> str
        Returns a hex colour for each bar.
        e.g. lambda v, avg: '#d62728' if v < avg - 3 else '#2ca02c' if v > avg + 3 else '#7f7f7f'
    ref_line_fn : callable(zone_df) -> tuple(float, str) or None
        Returns (ref_value, label_str) for the dashed zone-average line,
        or None to suppress the line.
        e.g. lambda zdf: (zdf[value_col].mean(), f'Zone avg {zdf[value_col].mean():.1f}%')
    annotation_fn : callable(val, extra) -> str
        Formats the end-of-bar annotation. 'extra' is the value from
        annotation_extra_col (or None if not supplied).
        e.g. lambda v, t: f'{v:.1f}%  ({t:,}t)'
    annotation_extra_col : str, optional
        Secondary column passed as 'extra' to annotation_fn.
    top_n : int
        How many cities to show per zone (sorted by sort_col descending).
    sort_col : str, optional
        Column to sort cities by before taking top_n. Defaults to value_col.
    figsize : tuple
    save_path : str, optional
        If provided, saves the figure to this path (dpi=150, bbox_inches='tight').

    Example — SLA by city
    ---------------------
    plot_city_metric_by_zone(
        city_summary, ZONE_ORDER,
        value_col   = 'SLA_pct',
        title_tmpl  = '{zone} — City SLA Compliance\\n(top 15 by volume)',
        xlabel      = 'SLA Compliance (%)',
        color_fn    = lambda v, avg: ('#d62728' if v < avg-3 else
                                      '#2ca02c' if v > avg+3 else '#7f7f7f'),
        ref_line_fn = lambda zdf: (zone_sla[zone], f'Zone avg {zone_sla[zone]:.1f}%'),
        annotation_fn        = lambda v, t: f'{v:.1f}%  ({t:,}t)',
        annotation_extra_col = 'Tickets',
        sort_col             = 'Tickets',
        save_path = 'reports/figures/project4_ncr/13_city_sla_by_zone.png',
    )
    """
    if sort_col is None:
        sort_col = value_col

    fig, axes = plt.subplots(2, 3, figsize=figsize)
    fig.patch.set_facecolor('#fafafa')
    axes = axes.flatten()

    for ax_i, zone in enumerate(zone_order):
        ax  = axes[ax_i]
        zdf = city_df[city_df['ZONE'] == zone].copy()
        zdf = zdf.sort_values(sort_col, ascending=False).head(top_n)

        if zdf.empty:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center',
                    transform=ax.transAxes, fontsize=11)
            ax.set_title(title_tmpl.format(zone=zone),
                         fontsize=10, fontweight='bold', pad=6)
            continue

        # Reference line
        ref = ref_line_fn(zdf) if ref_line_fn else None
        zone_avg = ref[0] if ref else None

        colors = [color_fn(v, zone_avg) for v in zdf[value_col]]
        bars   = ax.barh(zdf['CITY'], zdf[value_col],
                         color=colors, edgecolor='white', linewidth=0.5)

        if ref is not None:
            ax.axvline(ref[0], color='#2c3e50', linestyle='--',
                       linewidth=1.2, label=ref[1])
            ax.legend(fontsize=8)

        extras = (zdf[annotation_extra_col].values
                  if annotation_extra_col else [None] * len(zdf))
        for bar, val, extra in zip(bars, zdf[value_col], extras):
            ax.text(bar.get_width() + 0.3,
                    bar.get_y() + bar.get_height() / 2,
                    annotation_fn(val, extra),
                    va='center', fontsize=7.5)

        ax.set_title(title_tmpl.format(zone=zone),
                     fontsize=10, fontweight='bold', pad=6)
        ax.set_xlabel(xlabel, fontsize=9)
        ax.invert_yaxis()
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

    plt.tight_layout(pad=2.0)
    if save_path:
        import os
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
    return fig
