# src/utils/__init__.py
"""
Shared Utility Functions
Used across analysis, plotting, and other modules
"""

import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Optional, List, Union


# ══════════════════════════════════════════════════════════════════════════════
# REGION UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def sort_by_region(df: pd.DataFrame, col: str = 'Region Name') -> pd.DataFrame:
    """
    Sort dataframe by region in canonical order (Region 1-5).
    
    Replaces 5+ repeated patterns of:
        region_order = ['Region 1', ..., 'Region 5']
        df[col] = pd.Categorical(df[col], categories=region_order, ordered=True)
        df = df.sort_values(col)
    
    Args:
        df: DataFrame with region column
        col: Name of region column (default 'Region Name')
        
    Returns:
        Sorted DataFrame with region as ordered categorical
    """
    from config import REGION_ORDER
    df = df.copy()
    df[col] = pd.Categorical(df[col], categories=REGION_ORDER, ordered=True)
    return df.sort_values(col)

# ══════════════════════════════════════════════════════════════════════════════
# REGION UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def sort_by_zone(df: pd.DataFrame, col: str = 'Zone Name') -> pd.DataFrame:
    """
    Sort dataframe by zone in canonical order (Zone 1-7).
    
    Args:
        df: DataFrame with zone column
        col: Name of zone column (default 'Zone Name')
        
    Returns:
        Sorted DataFrame with region as ordered categorical
    """
    from config import ZONE_ORDER
    df = df.copy()
    df[col] = pd.Categorical(df[col], categories=ZONE_ORDER, ordered=True)
    return df.sort_values(col)

# ══════════════════════════════════════════════════════════════════════════════
# PLOT UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def handle_empty_plot(
    ax, 
    title: str, 
    xlabel: str = '', 
    ylabel: str = '',
    message: Optional[str] = None
) -> None:
    """
    Display 'no data' message on empty plot with proper labels.
    
    Replaces 15+ repeated patterns of:
        if df.empty:
            ax.text(0.5, 0.5, 'No data...', ha='center', va='center', fontsize=12)
            ax.set_title(title)
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            return
    
    Args:
        ax: Matplotlib axes object
        title: Plot title
        xlabel: X-axis label (optional)
        ylabel: Y-axis label (optional)
        message: Custom message (defaults to 'No data available for {title}')
    """
    msg = message or f'No data available for {title}'
    ax.text(0.5, 0.5, msg, ha='center', va='center', fontsize=12)
    ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)


def save_figure(
    fig,
    filename: str,
    output_folder: str = 'output',
    dpi: int = 300,
    **kwargs
) -> str:
    """
    Save figure to output folder and log the path.
    
    Replaces 7+ repeated patterns of:
        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logging.info(f"Plot saved to: {output_path}")
    
    Args:
        fig: Matplotlib figure object (or plt for current figure)
        filename: Output filename (e.g., 'plot.png')
        output_folder: Directory to save to (default 'output')
        dpi: Resolution (default 300)
        **kwargs: Additional arguments passed to fig.savefig()
        
    Returns:
        Full path to saved file
    """
    os.makedirs(output_folder, exist_ok=True)
    filepath = os.path.join(output_folder, filename)
    fig.savefig(filepath, dpi=dpi, bbox_inches='tight', **kwargs)
    logging.info(f"Saved: {filepath}")
    return filepath


def configure_plot_axes(
    ax,
    title: str = '',
    xlabel: str = '',
    ylabel: str = '',
    rotation: int = 0
) -> None:
    """
    Apply common axis configuration to reduce repetition.
    
    Args:
        ax: Matplotlib axes object
        title: Plot title
        xlabel: X-axis label
        ylabel: Y-axis label
        rotation: X-tick label rotation angle (degrees)
    """
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if rotation:
        plt.setp(ax.get_xticklabels(), rotation=rotation, ha='right')


# ══════════════════════════════════════════════════════════════════════════════
# DATA UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def filter_top_n(
    df: pd.DataFrame, 
    col: str, 
    n: int,
    ascending: bool = False
) -> pd.DataFrame:
    """
    Return top N rows by value in specified column.
    
    Args:
        df: Input DataFrame
        col: Column to sort by
        n: Number of top rows to return
        ascending: Sort order (default False for top values)
        
    Returns:
        DataFrame with top N rows
    """
    return df.nlargest(n, col) if not ascending else df.nsmallest(n, col)


def calculate_percentage(
    df: pd.DataFrame, 
    count_col: str, 
    total: Optional[int] = None,
    decimals: int = 1
) -> pd.DataFrame:
    """
    Add percentage column based on count column.
    
    Args:
        df: DataFrame with count column
        count_col: Name of count column
        total: Total for percentage calculation (defaults to sum of count_col)
        decimals: Number of decimal places (default 1)
        
    Returns:
        DataFrame with added 'Percentage' column
    """
    df = df.copy()
    if total is None:
        total = df[count_col].sum()
    df['Percentage'] = (df[count_col] / total * 100) if total > 0 else 0
    df['Percentage'] = df['Percentage'].round(decimals)
    return df


def aggregate_by_region(
    df: pd.DataFrame,
    metric_col: str,
    agg_func: str = 'mean',
    region_col: str = 'Region'
) -> pd.DataFrame:
    """
    Aggregate metric by region with proper sorting.
    
    Commonly used pattern: group by region, calculate mean/count, sort by region order.
    
    Args:
        df: Input DataFrame
        metric_col: Column to aggregate
        agg_func: Aggregation function ('mean', 'sum', 'count', etc.)
        region_col: Region column name (default 'Region')
        
    Returns:
        Aggregated DataFrame sorted by region order
    """
    result = df.groupby(region_col)[metric_col].agg(agg_func).reset_index()
    result.columns = [region_col, f'{metric_col.title()} ({agg_func.title()})']
    return sort_by_region(result, region_col)


# ══════════════════════════════════════════════════════════════════════════════
# TEXT UTILITIES (if shared outside cleaning)
# ══════════════════════════════════════════════════════════════════════════════

def truncate_text(text: str, max_length: int = 50, suffix: str = '...') -> str:
    """
    Truncate text to max length with suffix.
    
    Args:
        text: Input text
        max_length: Maximum length including suffix
        suffix: Suffix to add when truncated (default '...')
        
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def format_large_number(num: Union[int, float], decimals: int = 1) -> str:
    """
    Format large numbers with K/M/B suffixes.
    
    Args:
        num: Number to format
        decimals: Decimal places to show
        
    Returns:
        Formatted string (e.g., '1.2K', '3.4M')
    """
    if abs(num) >= 1e9:
        return f'{num/1e9:.{decimals}f}B'
    if abs(num) >= 1e6:
        return f'{num/1e6:.{decimals}f}M'
    if abs(num) >= 1e3:
        return f'{num/1e3:.{decimals}f}K'
    return str(num)
