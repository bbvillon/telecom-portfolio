"""
Fault Ticket KPI Calculations
Reusable metrics across all projects
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional


def calculate_mttr(df: pd.DataFrame, group_by: Optional[str] = None) -> pd.DataFrame:
    """Calculate Mean Time To Repair (MTTR)."""
    if group_by:
        return df.groupby(group_by)['OUTAGEDURATION'].mean().reset_index(name='MTTR')
    return pd.DataFrame({'MTTR': [df['OUTAGEDURATION'].mean()]})


def calculate_sla_compliance(df: pd.DataFrame, group_by: Optional[str] = None) -> pd.DataFrame:
    """Calculate SLA compliance rate."""
    if group_by:
        sla = df.groupby(group_by)['SLA_Compliant'].agg(['mean', 'count']).reset_index()
        sla['SLA_Compliance_Rate'] = sla['mean'] * 100
        sla['Ticket_Count'] = sla['count']
        return sla[[group_by, 'SLA_Compliance_Rate', 'Ticket_Count']]
    rate = df['SLA_Compliant'].mean() * 100
    return pd.DataFrame({'SLA_Compliance_Rate': [rate], 'Ticket_Count': [len(df)]})


def calculate_fault_density(df: pd.DataFrame, group_by: str) -> pd.DataFrame:
    """Calculate faults per site (fault density)."""
    grouped = df.groupby(group_by).agg({
        'TICKETID': 'count',
        'Area': 'nunique'
    }).reset_index()
    grouped.columns = [group_by, 'Total_Faults', 'Unique_Sites']
    grouped['Fault_Density'] = grouped['Total_Faults'] / grouped['Unique_Sites']
    return grouped

def calculate_noc_time(df: pd.DataFrame, group_by: Optional[str] = None) -> pd.DataFrame:
    """
    Calculate average NOC/ROC troubleshooting time per group.

    NOC/ROC Time = DISPATCHDATE - REPORTDATE
        Time between NOC opening the ticket (REPORTDATE) and endorsing to field (DISPATCHDATE).
        Zero is a valid value — some faults are immediately identified as field-only.

    Tickets flagged as Timestamp_Integrity=False are excluded — these have DISPATCHDATE
    recorded after RESOLVEDDATE (late dispatch entry in source system) and would
    produce nonsense averages if included.

    Previously labelled Avg_Dispatch_Delay. Renamed for operational clarity.
    """
    # Exclude timestamp-corrupt tickets from phase aggregations
    if 'Timestamp_Integrity' in df.columns:
        df = df[df['Timestamp_Integrity']]

    if 'DISPATCH_DELAY_HOURS' not in df.columns:
        if 'DISPATCHDATE' in df.columns and 'REPORTDATE' in df.columns:
            df = df.copy()
            df['DISPATCHDATE'] = pd.to_datetime(df['DISPATCHDATE'], errors='coerce')
            df['REPORTDATE']   = pd.to_datetime(df['REPORTDATE'],   errors='coerce')
            df['DISPATCH_DELAY_HOURS'] = (
                (df['DISPATCHDATE'] - df['REPORTDATE']).dt.total_seconds() / 3600
            )
        else:
            col = 'Avg_NOC_Time'
            if group_by:
                return pd.DataFrame({group_by: df[group_by].unique(), col: [0.0] * df[group_by].nunique()})
            return pd.DataFrame({col: [0.0]})

    if group_by:
        return df.groupby(group_by)['DISPATCH_DELAY_HOURS'].mean().reset_index(name='Avg_NOC_Time')
    return pd.DataFrame({'Avg_NOC_Time': [df['DISPATCH_DELAY_HOURS'].mean()]})


def calculate_field_time(df: pd.DataFrame, group_by: Optional[str] = None) -> pd.DataFrame:
    """
    Calculate average field engineer resolution time per group.

    Field Time = RESOLVEDDATE - DISPATCHDATE (= FIELD_TIME_HOURS in cleaned output)
        Time from WO issuance to ticket close.

    Tickets flagged as Timestamp_Integrity=False are excluded — these have DISPATCHDATE
    recorded after RESOLVEDDATE producing negative field time values.

    Note: In high-CBD zones (Makati, BGC, Ortigas) this may include building
    work permit wait time which leaves no distinct RFO tag. Treat as upper bound
    on true engineering resolution time for those zones.
    """
    # Exclude timestamp-corrupt tickets from phase aggregations
    if 'Timestamp_Integrity' in df.columns:
        df = df[df['Timestamp_Integrity']]

    # Prefer pre-computed column from pipeline — avoids re-parsing timestamps
    if 'FIELD_TIME_HOURS' in df.columns:
        if group_by:
            return df.groupby(group_by)['FIELD_TIME_HOURS'].mean().reset_index(name='Avg_Field_Time')
        return pd.DataFrame({'Avg_Field_Time': [df['FIELD_TIME_HOURS'].mean()]})

    # Fallback: compute from timestamps if FIELD_TIME_HOURS absent (e.g. older cleaned files)
    df = df.copy()
    for col in ['RESOLVEDDATE', 'DISPATCHDATE']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'RESOLVEDDATE' in df.columns and 'DISPATCHDATE' in df.columns:
        df['_field_time'] = (
            (df['RESOLVEDDATE'] - df['DISPATCHDATE']).dt.total_seconds() / 3600
        )
    elif 'OUTAGEDURATION' in df.columns and 'DISPATCH_DELAY_HOURS' in df.columns:
        df['_field_time'] = df['OUTAGEDURATION'] - df['DISPATCH_DELAY_HOURS']
    else:
        col = 'Avg_Field_Time'
        if group_by:
            return pd.DataFrame({group_by: df[group_by].unique(),
                                 col: [0.0] * df[group_by].nunique()})
        return pd.DataFrame({col: [0.0]})

    if group_by:
        return df.groupby(group_by)['_field_time'].mean().reset_index(name='Avg_Field_Time')
    return pd.DataFrame({'Avg_Field_Time': [df['_field_time'].mean()]})


def calculate_kpis(df: pd.DataFrame, group_by: str = 'ZONE') -> Dict[str, pd.DataFrame]:
    """
    Calculate all key KPIs for a given grouping.
    
    Returns:
        Dictionary of KPI DataFrames
    """
    return {
        'mttr'         : calculate_mttr(df, group_by),
        'sla'          : calculate_sla_compliance(df, group_by),
        'volume'       : df.groupby(group_by).size().reset_index(name='Ticket_Count'),
        'fault_density': calculate_fault_density(df, group_by),
        'noc_time'     : calculate_noc_time(df, group_by),
        'field_time'   : calculate_field_time(df, group_by),
    }


#def calculate_regional_summary(df: pd.DataFrame) -> pd.DataFrame:
#    """Generate comprehensive regional summary table."""
#    kpis = calculate_kpis(df, 'Region')
#    
#    summary = kpis['volume'].copy()
#    summary = summary.merge(kpis['mttr'], on='Region')
#    summary = summary.merge(kpis['sla'][['Region', 'SLA_Compliance_Rate']], on='Region')
#    summary = summary.merge(kpis['fault_density'], on='Region')
#    summary = summary.merge(kpis['dispatch_delay'], on='Region')
#    
#    return summary.sort_values('Ticket_Count', ascending=False)

def calculate_zone_summary(df: pd.DataFrame, exclude_unknown=True) -> pd.DataFrame:
    """Generate comprehensive zone summary table."""
    if exclude_unknown:
        df = df[df['ZONE'] != 'Unknown']   
        
    kpis = calculate_kpis(df, 'ZONE')
    
    summary = kpis['volume'].copy()
    summary = summary.merge(kpis['mttr'], on='ZONE')
    summary = summary.merge(kpis['sla'][['ZONE', 'SLA_Compliance_Rate']], on='ZONE')
    summary = summary.merge(kpis['fault_density'], on='ZONE')
    summary = summary.merge(kpis['noc_time'], on='ZONE')
    summary = summary.merge(kpis['field_time'], on='ZONE')
    
    return summary.sort_values('Ticket_Count', ascending=False)
