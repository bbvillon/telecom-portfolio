"""
Fault Ticket Analysis Functions
Reusable aggregations and statistical analyses
"""

import pandas as pd
import numpy as np
from typing import List, Optional


def aggregate_by_region(df: pd.DataFrame, metric_col: str, agg_func: str = 'mean') -> pd.DataFrame:
    """Aggregate metric by region with proper sorting."""
    import sys
    sys.path.insert(0, '../..')
    from src.utils import sort_by_region
    
    result = df.groupby('Region')[metric_col].agg(agg_func).reset_index()
    result.columns = ['Region', f'{metric_col}_{agg_func}']
    return sort_by_region(result, 'Region')


def analyze_top_fault_types(df: pd.DataFrame, by: str = 'Region', top_n: int = 5) -> pd.DataFrame:
    """Analyze top N fault types per group."""
    result = df.groupby([by, 'Standardized RFO']).size().reset_index(name='Count')
    result['Rank'] = result.groupby(by)['Count'].rank(ascending=False, method='first')
    return result[result['Rank'] <= top_n].sort_values([by, 'Rank'])


def identify_outliers(df: pd.DataFrame, metric_col: str, group_by: str, 
                      threshold: float = 1.5) -> pd.DataFrame:
    """Identify outlier groups using IQR method."""
    grouped = df.groupby(group_by)[metric_col].mean().reset_index()
    
    Q1 = grouped[metric_col].quantile(0.25)
    Q3 = grouped[metric_col].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - threshold * IQR
    upper = Q3 + threshold * IQR
    
    outliers = grouped[(grouped[metric_col] < lower) | (grouped[metric_col] > upper)]
    outliers['Outlier_Type'] = outliers[metric_col].apply(
        lambda x: 'Low' if x < lower else 'High'
    )
    
    return outliers
