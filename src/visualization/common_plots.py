"""
Common Plotting Functions
Generic, reusable chart types
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../..')
from config import PROFESSIONAL_PALETTE


def create_bar_graph(ax, data, x_col, y_col, title="", x_label="", y_label="", 
                     palette=PROFESSIONAL_PALETTE, top_n=None, rotation=45, show_value=False):
    """
    Generic bar graph with optional annotations.
    """
    plot_data = data.copy()
    
    if top_n:
        plot_data = plot_data.nlargest(top_n, y_col)
    elif not isinstance(plot_data[x_col].dtype, pd.CategoricalDtype):
        plot_data = plot_data.sort_values(by=y_col, ascending=False)

    if plot_data.empty:
        ax.text(0.5, 0.5, f'No data available for {title}', ha='center', va='center', fontsize=12)
        ax.set_title(title)
        return

    unique_x = len(plot_data[x_col].unique())
    effective_palette = palette[:unique_x] if len(palette) >= unique_x else palette

    # Ensure palette is a valid list or dict for Seaborn
    if isinstance(palette, dict):
        effective_palette = palette
    else:
        # Slice only if it's a list
        unique_x = plot_data[x_col].nunique()
        effective_palette = list(palette)[:unique_x]

    # The Fix: Explicitly define hue_order and palette mapping
    sns.barplot(
        data=plot_data, 
        x=x_col, 
        y=y_col, 
        ax=ax, 
        palette=effective_palette, 
        hue=x_col,            # Must be present
        hue_order=plot_data[x_col].unique(), # Fixes the Warning
        legend=False          # Keeps it clean
    )

    if show_value:
        for i, bar in enumerate(ax.patches):
            height = bar.get_height()
            if height > 0 and i < len(plot_data):
                ax.text(bar.get_x() + bar.get_width()/2, height,
                       f'{int(height)}', ha='center', va='bottom', fontsize=9)

    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    plt.setp(ax.get_xticklabels(), rotation=rotation, ha='right')
    ax.grid(axis='y', alpha=0.3)


def plot_heatmap(ax, df, x_col=None, y_col=None, value_col='Count',
                 title='Heatmap', cmap='YlOrRd', annot=True, fmt='g',
                 vmin=None, vmax=None, mask=None,
                 cbar_label='', cbar_shrink=0.8,
                 x_label='', y_label='',
                 x_rotation=45, y_rotation=0,
                 pivot=True):
    """
    Generic heatmap visualization.

    Parameters
    ----------
    pivot : bool
        True  (default) — df is long-form; pivots using x_col/y_col/value_col.
        False           — df is already a pivot table (index=rows, columns=cols).
                          x_col/y_col/value_col are ignored in this mode.
    vmin, vmax : float, optional
        Colour scale bounds passed to sns.heatmap.
    mask : DataFrame, optional
        Boolean mask (same shape as pivot); masked cells are left blank.
    cbar_label : str
        Label for the colour bar.
    cbar_shrink : float
        Fraction to shrink the colour bar (passed to cbar_kws).
    x_label, y_label : str
        Axis labels.
    x_rotation, y_rotation : int
        Tick label rotation angles.

    Example — already-pivoted DataFrame
    ------------------------------------
    plot_heatmap(ax, sla_pu, pivot=False,
                 title='SLA by Priority × Zone',
                 cmap='RdYlGn', fmt='.1f',
                 vmin=60, vmax=100, mask=sla_pu.isna(),
                 cbar_label='SLA (%)', cbar_shrink=0.8,
                 x_label='Priority Tier', y_label='Zone',
                 x_rotation=0, y_rotation=0)
    """
    if pivot:
        if df.empty:
            ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
            return
        data = df.pivot_table(
            index=y_col, columns=x_col,
            values=value_col, aggfunc='sum', fill_value=0
        )
    else:
        data = df

    cbar_kws = {'label': cbar_label, 'shrink': cbar_shrink} if cbar_label else {'shrink': cbar_shrink}

    sns.heatmap(
        data, annot=annot, fmt=fmt, cmap=cmap, ax=ax,
        vmin=vmin, vmax=vmax, mask=mask,
        linewidths=0.4, linecolor='#eee',
        cbar_kws=cbar_kws,
    )
    ax.set_title(title, fontsize=11, fontweight='bold', pad=10)
    ax.set_xlabel(x_label, fontsize=10)
    ax.set_ylabel(y_label, fontsize=10)
    ax.tick_params(axis='x', rotation=x_rotation)
    ax.tick_params(axis='y', rotation=y_rotation)
