"""
Data Cleaning Module for Telecom Fault Ticket Analysis
5-phase pipeline: Filtering → Repair → Geographic → Operational → Privacy
"""

import numpy as np
import pandas as pd
import logging
import os
import re
import hashlib
import time
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import process, fuzz
from config import output_folder, EXPECTED_RFO_VALUES, SLA_THRESHOLDS, get_logger
from loading import load_site_database

log = get_logger('pipeline')


# ── NE_CATEGORY MAP (module-level: defined once, not rebuilt per row) ─────────
_NE_CATEGORY_MAP = {
    'Access':            ['CELL', 'BTS', 'NODEB', 'ENODEB', 'GNODEB', 'GPON', 'MSAN',
                          'AN', 'ANTENNA', '2G REPEATER', 'HYBRID_A', 'HYBRID_B'],
    'Transport':         ['SDH_MUX', 'SDH MUX', 'SDH_RADIO', 'PDH', 'DWDM', 'MW'],
    'Core Network':      ['MSS', 'MGW', 'USN', 'BSC', 'RNC', 'CGNAT', 'GGSN',
                          'MCO', 'VCGW', 'ASR', 'AG', 'ESS'],
    'IP/Network Infra':  ['ROUTER', 'PE_ROUTER', 'P_ROUTER', 'SWITCH', 'FIREWALL',
                          'L7_FIREWALL', 'SBC', 'BNG', 'GEOROAM'],
    'Data Services':     ['AAA', 'PCRF', 'DMS', 'HSS', 'SPG', 'USCDB',
                          'SEC_GW', 'WLN DHCP', 'CSCF', 'SCP'],
    'Network Management':['NMS', 'SERVER', 'UAC', 'CGPOMU'],
    'Power/Infra':       ['POWER FACILITY', 'ATS'],
    'Other/Unknown':     ['UNKNOWN'],
}
# Flattened lookup: 'CELL' → 'Access'
_NE_TYPE_LOOKUP = {
    t.upper(): cat for cat, types in _NE_CATEGORY_MAP.items() for t in types
}

# NEType → RFO fallback (extended: covers power, transport, IP, and fiber NETypes)
_NE_RFO_FALLBACK = {
    'CELL': 'EQUIPMENT-Defective Hardware',    'BTS':  'EQUIPMENT-Defective Hardware',
    'NODEB': 'EQUIPMENT-Defective Hardware',   'ENODEB': 'EQUIPMENT-Defective Hardware',
    'GNODEB': 'EQUIPMENT-Defective Hardware',
    'DWDM': 'TRANSMISSION-Equipment Problem',  'SDH_MUX': 'TRANSMISSION-Equipment Problem',
    'SDH_RADIO': 'TRANSMISSION-Fiber Problem', 'MW': 'TRANSMISSION-Fiber Problem',
    'MSAN': 'TRANSMISSION-Fiber Problem',      'GPON': 'TRANSMISSION-Fiber Problem',
    'AG': 'TRANSMISSION-Fiber Problem',
    'ROUTER': 'TRANSMISSION-IP Network Problem', 'PE_ROUTER': 'TRANSMISSION-IP Network Problem',
    'POWER FACILITY': 'FACILITIES-Power Failure', 'ATS': 'FACILITIES-Power Failure',
    'HYBRID_B': 'UNKNOWN-Under Investigation',
}

# FOC description lists (lowercased, dash-normalised — matched against rfo.lower())
_FOC_LINEAR = [
    'foc cut linear', 'foc cut  linear', 'single fiber cut', 'fiber optic cut linear',
    'transmission foc cut', 'spliced linear', 'fiber break linear', 'single link failure',
]
_FOC_REDUNDANT = [
    'foc cut with redundancy', 'foc cut  redundancy', 'multiple fiber cut',
    'redundant link failure', 'foc cut protected', 'spliced redundancy',
    'multiple access link failure',
]
_FOC_GENERIC = [
    'pfocn fiber', 'others animal problem', 'transmission foc cut burnt cable',
    'foc cut multiple cut', 'transmission foc cut electrical discharge',
    'transmission foc cut fiber was dragged by vehicle', 'transmission foc cut intentional cut',
    'transmission foc cut lashing wire', 'transmission foc cut other telco',
    'transmission foc cut rat bites', 'transmission foc cut road construction',
    'transmission foc cut submarine cut', 'transmission foc cut tree fallen',
    'transmission foc cut tree trimming', 'transmission foc cut vehicular accident',
    'transmission foc cut animal problem', 'fbc', 'fiber break', 'fiber cut',
    'foc issue', 'fiber optic issue', 'fiber problem',
]

# Keyword mapping for fuzzy/keyword matching (25 RFO categories, all as lowercase)
_KEYWORD_MAP = {
    'FOC CUT - LINEAR':     _FOC_LINEAR + ['single fiber outage', 'linear fiber break'],
    'FOC CUT WITH REDUNDANCY': _FOC_REDUNDANT + ['redundant fiber outage', 'multiple fiber break'],
    'ADMIN-Lessor Related Cause': [
        'site access restriction', 'community opposition', 'extortion', 'genset noise',
        'unsettled electric bill', 'late payment', 'unendorsed site utility notification form',
        'lessor lease issue', 'non renewal of contract', 'billing issue', 'swat site',
        'prime site genset rest', 'access restriction lessor', 'lessor', 'admin', 'contract',
        'site access', 'lease dispute', 'landlord issue', 'access denied',
    ],
    'EQUIPMENT-Configuration Problem': [
        'configuration', 'reconfigured', 'config', 'misconfiguration', 'config error', 'setting issue',
    ],
    'EQUIPMENT-Decommissioned': [
        'decomissioned', 'decommissioned', 'to be decommissioned', 'site shutdown',
    ],
    'EQUIPMENT-Defective Hardware': [
        'ops advance', 'equipment antenna', 'equipment cable', 'card module', 'equipment connector',
        'equipment design', 'equipment port', 'ht rru', 'replaced', 'hardware reset',
        'antenna system problem', 'loose defective connector', 'card failure', 'module failure',
        'defective', 'bts antenna', 'antenna', 'hardware failure', 'equipment failure',
        'faulty hardware', 'device failure', 'rru issue',
    ],
    'EQUIPMENT-Software Problem': [
        'software', 'software conflict', 'card level software', 'firmware issue',
        'software bug', 'software crash',
    ],
    'FACILITIES-Cooling System Problem': [
        'cooling', 'aircon', 'temperature', 'ac unit failure', 'cooling failure',
    ],
    'FACILITIES-Design Problem': ['facilities design', 'design issue', 'infrastructure design'],
    'FACILITIES-Monitoring Equipment Problem': [
        'rms', 'monitoring', 'monitoring failure', 'sensor issue',
    ],
    'FACILITIES-Power Failure': [
        'facilities cable', 'power', 'facilities connector', 'ats', 'rectifier',
        'ac mains failure', 'batteries', 'self restored commercial power resumed',
        'power outage', 'dc under voltage alarm', 'power supply abnormal', 'spd fault alarm',
        'dc breaker tripped', 'genset failure', 'power supply issue', 'power loss',
        'battery failure', 'power disruption', 'mains failure', 'commercial power fluctuation',
    ],
    'FACILITIES-Water Seepage': ['facilities water', 'water seepage', 'leak', 'flooding'],
    'OTHERS-Force Majeure': [
        'force', 'force majeure', 'natural disaster', 'typhoon', 'earthquake', 'flood',
    ],
    'Invalid': ['invalid', 'test3'],
    'OTHERS-Preventive Maintenance': [
        'preventive maintenance', 'pm', 'scheduled maintenance', 'routine maintenance',
    ],
    'OTHERS-Security Breach': [
        'security', 'sabotage', 'pilferage', 'theft', 'unauthorized access', 'vandalism',
    ],
    'THIRD PARTY-Activity Related': [
        'activity', 'endorsed activity', 'activity related', 'activity triggered',
        'unendorsed activity', 'third party activity', 'construction activity', 'road work',
    ],
    'THIRD PARTY-POI': ['last mile', 'poi', 'point of interconnect', 'third party interconnect'],
    'TRANSMISSION-Cable Problem': [
        'transmission cable', 'transmission connector', 'cable issue', 'cable damage',
    ],
    'TRANSMISSION-Capacity Problem': [
        'transmission capacity', 'capacity issue', 'congested', 'congestion',
    ],
    'TRANSMISSION-Equipment Problem': [
        'transmission card module', 'transmission port problem', 'port problem', 'transmission design',
    ],
    'TRANSMISSION-IP Network Problem': [
        'ip network', 'logical trail', 'ip issue', 'network connectivity',
        'connectivity issue', 'link down', 'network outage',
    ],
    'TRANSMISSION-Microwave Antenna System Problem': [
        'transmission microwave', 'transmission rf', 'microwave issue', 'microwave failure',
    ],
    'TRANSMISSION-Offnetwork': ['offnetwork', 'off network'],
    'TRANSMISSION-Fiber Problem': [
        'odf', 'high loss', 'kinked fiber', 'foc high loss', 'tt',
        'fiber degradation', 'fiber attenuation',
    ],
    'UNKNOWN-Under Investigation': [
        'inquiry', 'unknown', 'bau open', 'under investigation',
        'work in progress', 'self restored under investigation', 'tba',
    ],
}


# ── PRIVATE HELPERS ────────────────────────────────────────────────────────────

def _save(df_or_series, filename, **kwargs):
    """Save any DataFrame/Series to output_folder without repeating os.path.join."""
    path = os.path.join(output_folder, filename)
    df_or_series.to_csv(path, index=False, **kwargs)
    log.info(f"Saved {len(df_or_series)} rows → {filename}")
    return path


def _filter_and_log(df: pd.DataFrame, mask: pd.Series, step: str) -> pd.DataFrame:
    """Apply boolean mask, log rows dropped, return filtered df."""
    before = len(df)
    df = df[mask]
    log.info(f"{step}: dropped {before - len(df):,} rows (remaining: {len(df):,})")
    return df


def _log_phase(name: str):
    log.info(f"\n{'─'*60}\n  {name}\n{'─'*60}")


# ── PUBLIC HELPER FUNCTIONS ───────────────────────────────────────────────────

def validate_initial_data_quality(df: pd.DataFrame) -> dict:
    """Pre-cleaning data quality snapshot. Returns dict with counts and quality score."""
    def _count(col, cond):
        return cond(df[col]).sum() if col in df.columns else 0

    report = {
        'total_records':       len(df),
        'duplicate_tickets':   df['TICKETID'].duplicated().sum() if 'TICKETID' in df.columns else 0,
        'blank_priority':      _count('Priority', lambda s: s.isna()),
        'invalid_priority':    _count('Priority', lambda s: ~s.isin([1, 2, 3, 4])),
        'invalid_urgency':     _count('Urgency',  lambda s: ~s.isin([0, 1, 2, 3])),
        'invalid_status':      _count('FT Status',lambda s: ~s.isin(['CLOSED','RESOLVED','PENRESOLVE','CANCELLED','INPROG'])),
        'invalid_fault_types': _count('Fault Type', lambda s: s == 'INVALID'),
    }
    issues = sum(v for k, v in report.items() if k != 'total_records')
    report['quality_score'] = round(100 - (issues / len(df) * 100), 2) if len(df) else 0
    return report


def validate_cleaned_data(df: pd.DataFrame) -> dict:
    """7-rule post-cleaning validation. Logs result and saves data_validation_report.csv."""
    checks = [
        ('Required columns present',
         lambda d: not [c for c in ['REPORTDATE','Priority_Urgency','OUTAGEDURATION',
                                    'Region','Standardized RFO','SLA_Compliant',
                                    'DISPATCH_DELAY_HOURS'] if c not in d.columns]),
        ('No duplicate tickets',
         lambda d: d['TICKETID'].duplicated().sum() == 0 if 'TICKETID' in d.columns else True),
        ('All regions valid',
         lambda d: d[~d['Region'].isin(['Region 1','Region 2','Region 3','Region 4','Region 5'])].empty),
        ('SLA_Compliant values valid',
         lambda d: d['SLA_Compliant'].dropna().isin([0, 1]).all() if 'SLA_Compliant' in d.columns else True),
        ('No negative outage durations',
         lambda d: (d['OUTAGEDURATION'] >= 0).all() if 'OUTAGEDURATION' in d.columns else True),
        ('Priority_Urgency format valid',
         lambda d: d['Priority_Urgency'].isin(list(SLA_THRESHOLDS.keys())).all() if 'Priority_Urgency' in d.columns else True),
        ('Resolution_Path values valid',
         lambda d: d['Resolution_Path'].isin(['Auto_Self_Restored','NOC_Remote_Restored',
                                              'Field_Dispatch_Restored']).all() if 'Resolution_Path' in d.columns else True),
    ]

    results = []
    for label, fn in checks:
        try:
            passed = bool(fn(df))
        except Exception as e:
            passed = False
        results.append({'check': label, 'passed': passed})
        log.info(f"  {'✓' if passed else '✗'} {label}")

    status = 'PASS' if all(r['passed'] for r in results) else 'FAIL'
    log.info(f"  Overall: {status}")
    _save(pd.DataFrame(results), 'data_validation_report.csv')
    return {'overall_status': status, 'validations': results}


def categorize_inference_confidence(score: float) -> str:
    """Map cosine similarity score to confidence tier.

    Thresholds recalibrated to this corpus where the practical score ceiling
    is ~0.50 (short technical telecom descriptions yield low absolute cosine
    similarity even for correct matches). Boundaries are percentile-based on
    the observed score distribution (mean=0.23, 75th pct=0.30, max=0.50):
      high     >= 0.40  top of corpus range, strong signal
      medium   >= 0.28  above 75th percentile, reasonable signal
      low      >= 0.15  above 25th percentile, weak but plausible
      very_low  < 0.15  bottom quartile, minimal vocabulary overlap
    """
    if score >= 0.40: return 'high'
    if score >= 0.28: return 'medium'
    if score >= 0.15: return 'low'
    return 'very_low'


def fuzzy_match_rfo(rfo_text: str, threshold: int = 85) -> str:
    """Fuzzy-match rfo_text against EXPECTED_RFO_VALUES. Returns 'Uncategorized' below threshold.
    Uses rapidfuzz WRatio scorer — handles token order and partial matches better than simple ratio.
    """
    if not rfo_text:
        return 'Uncategorized'
    match, score, _ = process.extractOne(rfo_text, EXPECTED_RFO_VALUES, scorer=fuzz.WRatio)
    return match if score >= threshold else 'Uncategorized'


# ── REGION ASSIGNMENT ─────────────────────────────────────────────────────────

def assign_real_area(row) -> str:
    """Assigns Region 1-5 from ContactGroup → Area → DESCRIPTION, or 'Unknown Area'."""
    cg   = str(row['ContactGroup']).upper() if pd.notna(row['ContactGroup']) else ''
    area = str(row['Area']).upper()         if pd.notna(row['Area'])         else ''
    desc = str(row['DESCRIPTION']).upper()  if pd.notna(row['DESCRIPTION'])  else ''

    # Priority 1: ContactGroup
    if cg:
        if 'VIS' in cg:               return 'Region 4'
        if 'NLZ' in cg:               return 'Region 1'
        if 'SLZ' in cg:               return 'Region 2'
        if any(x in cg for x in ['GMA','ALL']): return 'Region 3'
        if 'MIN' in cg:               return 'Region 5'

    # Priority 2: Area code
    if area:
        if 'NCR' in area:             return 'Region 3'
        if 'NL'  in area:             return 'Region 1'
        if 'SL'  in area:             return 'Region 2'
        if 'VIS' in area:             return 'Region 4'
        if 'MIN' in area:             return 'Region 5'

    # Priority 3: Description keywords
    if desc:
        kw_map = {
            'Region 1': ['NLZ','DUN','SCS','RXS','CBN','TRL','BTN','MABINIST'],
            'Region 2': ['SLZ','BTY','MAS','GNR','CMO','NSB','NSG','AUA'],
            'Region 3': ['GMA','NCR','SJN','VALERO','BAC','CLC','MLB','QCY','ERM','DELPAN'],
            'Region 4': ['DUM','LHG','VIZ','LAHUG','TLS','ORM','TABON','CDZ','JMB','KALIBO','BCD'],
            'Region 5': ['SURIG','CDO','ILG','ILI','DVO','DAPITAN','GEN','TAGUM'],
        }
        for region, keywords in kw_map.items():
            if any(k in desc for k in keywords):
                return region

    return 'Unknown Area'


# ── TEXT UTILITIES ─────────────────────────────────────────────────────────────

def clean_text(text) -> str:
    """Replace dashes/underscores with spaces and collapse whitespace."""
    if pd.isna(text):
        return text
    return re.sub(r'\s+', ' ', str(text).replace('-', ' ').replace('_', ' ')).strip()


def preprocess_text(text) -> str:
    """Lowercase, strip punctuation, remove stop words. Used for TF-IDF."""
    if pd.isna(text):
        return ''
    text = re.sub(r'[^a-z0-9\s]', ' ', str(text).lower())
    words = [w for w in text.split() if w not in ENGLISH_STOP_WORDS and len(w) > 2]
    return ' '.join(words)


# ── RFO STANDARDIZATION ───────────────────────────────────────────────────────

def standardize_rfo_description(row) -> str:
    """
    Map raw RFO text to a standard EXPECTED_RFO_VALUES label.

    Resolution cascade (first match wins):
      1. Exact match against EXPECTED_RFO_VALUES
      2. INVALID flag
      3. FOC description lists  (linear → redundant → generic)
      4. ActionTaken inference  (spliced, replaced, reconfigured, power restored)
      5. Keyword map scan       (25 categories, rfo + root_cause + action_taken)
      6. OTHERS/UNKNOWN description fallback  (9 description checks)
      7. NEType fallback map
      8. Final description keyword scan       (power / fiber / hardware)
      9. Fuzzy match against EXPECTED_RFO_VALUES
    """
    # ── Field extraction (lowercase for consistent matching) ──────────────────
    rfo    = '' if pd.isna(row.get('RFODescription')) else str(row['RFODescription']).lower().replace('-', ' ').strip()
    rc     = '' if pd.isna(row.get('RootCause'))      else str(row['RootCause']).lower()
    action = '' if pd.isna(row.get('ActionTaken'))    else str(row['ActionTaken']).lower()
    desc   = '' if pd.isna(row.get('DESCRIPTION'))    else str(row['DESCRIPTION']).lower()
    ne     = '' if pd.isna(row.get('NEType'))         else str(row['NEType']).upper().strip()
    urg    = row.get('Urgency', 3)

    # ── 1. Exact match ────────────────────────────────────────────────────────
    for ev in EXPECTED_RFO_VALUES:
        if rfo == ev.lower().replace('-', ' ').strip():
            return ev

    # ── 2. INVALID ────────────────────────────────────────────────────────────
    if 'invalid' in rfo or 'test3' in rfo:
        return 'Invalid'

    # ── 3. FOC description lists ──────────────────────────────────────────────
    def _foc_from_desc_and_urgency(desc, urg):
        """Resolve ambiguous FOC to linear/redundant using description + urgency."""
        if any(t in desc for t in ['multiple access link failure', 'redundant link']) or urg > 1:
            return 'FOC CUT WITH REDUNDANCY'
        if 'single link failure' in desc or urg == 1:
            return 'FOC CUT - LINEAR'
        return 'TRANSMISSION-Fiber Problem'

    if any(fc in rfo for fc in _FOC_LINEAR) or 'single link failure' in desc:
        return 'FOC CUT - LINEAR'
    if any(fc in rfo for fc in _FOC_REDUNDANT) or any(t in desc for t in ['multiple access link failure', 'redundant link']):
        return 'FOC CUT WITH REDUNDANCY'
    if any(fc in rfo for fc in _FOC_GENERIC) or any(t in desc for t in ['fiber optic', 'fiber issue', 'foc problem']):
        return _foc_from_desc_and_urgency(desc, urg)

    # ── 4. ActionTaken inference ──────────────────────────────────────────────
    if 'spliced' in action:
        return _foc_from_desc_and_urgency(desc, urg)
    if 'replaced' in action or 'hardware reset' in action:
        return 'EQUIPMENT-Defective Hardware'
    if 'reconfigured' in action:
        return 'EQUIPMENT-Configuration Problem'
    if 'self restored commercial power resumed' in action:
        return 'FACILITIES-Power Failure'

    # ── 5. Keyword map scan (rfo + root_cause + action_taken combined) ────────
    combined = f'{rfo} {rc} {action}'
    for label, keywords in _KEYWORD_MAP.items():
        if any(kw in combined for kw in keywords):
            return label

    # ── 6. OTHERS/UNKNOWN description fallback ────────────────────────────────
    if any(t in rfo for t in ['others complaint', 'unknown under investigation', 'unknown']):
        if any(t in desc for t in ['communication with the device failed', 'device unreachable']):
            return 'TRANSMISSION-IP Network Problem'
        if any(t in desc for t in ['ac mains failure', 'dc under voltage', 'power supply issue',
                                   'power outage', 'genset failure']):
            return 'FACILITIES-Power Failure'
        if 'ne is disconnected' in desc or 'site access' in desc:
            return 'ADMIN-Lessor Related Cause'
        if any(t in desc for t in ['site connectivity', 'link down', 'connectivity issue', 'network drop']):
            return 'TRANSMISSION-IP Network Problem'
        if any(t in desc for t in ['equipment failure', 'hardware issue', 'rru issue', 'module failure']):
            return 'EQUIPMENT-Defective Hardware'
        if any(t in desc for t in ['configuration', 'misconfiguration', 'config error']):
            return 'EQUIPMENT-Configuration Problem'
        if any(t in desc for t in ['cooling', 'aircon failure', 'temperature issue']):
            return 'FACILITIES-Cooling System Problem'
        if any(t in desc for t in ['water seepage', 'leak', 'flooding']):
            return 'FACILITIES-Water Seepage'
        if any(t in desc for t in ['construction', 'road work', 'third party activity']):
            return 'THIRD PARTY-Activity Related'

    # ── 7. NEType fallback map ────────────────────────────────────────────────
    if ne in _NE_RFO_FALLBACK:
        return _NE_RFO_FALLBACK[ne]

    # ── 8. Final description keyword scan ─────────────────────────────────────
    if any(t in desc for t in ['power', 'battery', 'genset', 'rectifier']):
        return 'FACILITIES-Power Failure'
    if any(t in desc for t in ['fiber', 'foc', 'optic', 'cable']):
        return 'TRANSMISSION-Fiber Problem'
    if any(t in desc for t in ['hardware', 'equipment', 'module', 'card']):
        return 'EQUIPMENT-Defective Hardware'

    # ── 9. Fuzzy match as final resort ────────────────────────────────────────
    return fuzzy_match_rfo(rfo)


# ── TF-IDF INFERENCE ──────────────────────────────────────────────────────────

def infer_missing_values_tfidf(
    df: pd.DataFrame,
    target_col: str = 'NEType',
    description_col: str = 'DESCRIPTION',
    unknown_fill_value: str = 'UNKNOWN',
    enable_logging: bool = False,
    ticket_id_col: str = 'TICKETID',
    other_cols_for_log: list = None,
):
    """
    Fill NaN values in target_col using TF-IDF cosine similarity on description_col.
    Returns (df, log_df) if enable_logging else df.
    """
    if other_cols_for_log is None:
        other_cols_for_log = ['DESCRIPTION', 'NEType']

    for col in [description_col, target_col]:
        if col not in df.columns:
            raise ValueError(f"Missing required column: '{col}'")

    known_df   = df[df[target_col].notna()].copy()
    unknown_df = df[df[target_col].isna()].copy()

    empty_log = pd.DataFrame()

    if unknown_df.empty:
        log.info(f"No missing {target_col} values.")
        return (df, empty_log) if enable_logging else df

    if known_df.empty:
        df.loc[df[target_col].isna(), target_col] = unknown_fill_value
        return (df, empty_log) if enable_logging else df

    known_df['_desc']   = known_df[description_col].apply(preprocess_text)
    unknown_df['_desc'] = unknown_df[description_col].apply(preprocess_text)
    known_df   = known_df[known_df['_desc'].str.strip() != '']
    unknown_df = unknown_df[unknown_df['_desc'].str.strip() != '']

    if unknown_df.empty:
        df.loc[df[target_col].isna(), target_col] = unknown_fill_value
        return (df, empty_log) if enable_logging else df

    grouped = known_df.groupby(target_col)['_desc'].apply(' '.join).reset_index()
    corpus  = list(grouped['_desc']) + list(unknown_df['_desc'])
    matrix  = TfidfVectorizer().fit_transform(corpus)
    sims    = cosine_similarity(matrix[len(grouped):], matrix[:len(grouped)])

    assigned = grouped[target_col].iloc[np.argmax(sims, axis=1)].values
    scores   = np.max(sims, axis=1)
    df.loc[unknown_df.index, target_col] = assigned

    if not enable_logging:
        return df

    log_cols   = [c for c in [ticket_id_col, description_col, target_col] + other_cols_for_log
                  if c in df.columns]
    inferred_log = df.loc[unknown_df.index, log_cols].copy()
    inferred_log['Raw Inferred Value'] = assigned
    inferred_log['Similarity_Score']   = scores
    inferred_log['Confidence']         = inferred_log['Similarity_Score'].apply(categorize_inference_confidence)

    conf_dist = inferred_log['Confidence'].value_counts()
    log.info(f"  Confidence for {target_col}: " +
                 ", ".join(f"{t}={n}" for t, n in conf_dist.items()))

    _save(inferred_log, f'inferred_{target_col.lower()}_scores.csv')

    low = inferred_log[inferred_log['Confidence'].isin(['low', 'very_low'])]
    if not low.empty:
        _save(low, f'low_confidence_{target_col.lower()}.csv')
        log.warning(f"  {len(low)} low-confidence {target_col} inferences flagged")

    return df, inferred_log


# ── SLA COMPLIANCE ────────────────────────────────────────────────────────────

def determine_sla_compliance(row, priority_col='Priority_Urgency', time_col='RESOLUTION_TIME_HOURS'):
    """Return 1 (compliant), 0 (breach), or NaN (invalid) using centralised SLA_THRESHOLDS."""
    pu   = str(row[priority_col]).strip()
    hrs  = row[time_col]
    if pd.isna(hrs) or pd.isna(pu):
        return np.nan
    return 1 if hrs <= SLA_THRESHOLDS.get(pu, 24) else 0


# ── MAIN PIPELINE ─────────────────────────────────────────────────────────────

def clean_data(df: pd.DataFrame, region_scope=None, save_output: bool = True) -> pd.DataFrame:
    """
    Full 5-phase cleaning pipeline for telecom fault tickets.

    Phase 1 – Structural Filtering
    Phase 2 – Data Repair & Field Reconstruction
    Phase 3 – Geographic & Site Enrichment
    Phase 4 – Operational Modeling
    Phase 5 – Privacy & Output Protection
    """
    if df.empty:
        raise ValueError('Input dataset is empty.')

    start       = time.time()
    initial_rows = len(df)

    # Pre-clean quality report
    qr = validate_initial_data_quality(df)
    log.info(f"Pre-clean quality: {qr['total_records']:,} rows | score={qr['quality_score']:.1f} | "
                 f"dupes={qr['duplicate_tickets']} | blank_priority={qr['blank_priority']}")

    # Snapshot before any changes
    snap_cols = [c for c in ['TICKETID','Priority','Urgency','FT Status','Fault Type','NEType','RFODescription'] if c in df.columns]
    _save(df[snap_cols].head(1000), 'snapshot_pre_cleaning.csv')
   
    # ── PHASE 1: STRUCTURAL FILTERING ────────────────────────────────────────
    _log_phase("PHASE 1 – STRUCTURAL FILTERING")

    df = _filter_and_log(df, ~df['TICKETID'].duplicated(),               "Step 1 – drop duplicate TICKETID")
    df = _filter_and_log(df, df['Priority'].notna(),                     "Step 2 – drop blank Priority")
    df = _filter_and_log(df, df['Priority'].isin([1, 2, 3]),             "Step 3 – keep Priority 1-3")
    df['Urgency'] = pd.to_numeric(df['Urgency'], errors='coerce')
    df = _filter_and_log(df, df['Urgency'].isin([0, 1, 2, 3]),          "Step 4 – valid Urgency")
    df = _filter_and_log(df, df['FT Status'].isin(['CLOSED','RESOLVED','PENRESOLVE']), "Step 5 – valid FT Status")
    df = _filter_and_log(df, df['Fault Type'] != 'INVALID',             "Step 6 – drop Fault Type INVALID")

    _save(df[snap_cols].head(1000), 'snapshot_after_phase1.csv')

    # ── PHASE 2: DATA REPAIR & FIELD RECONSTRUCTION ───────────────────────────
    _log_phase("PHASE 2 – DATA REPAIR & FIELD RECONSTRUCTION")

    # Step 7 – Infer missing NEType via TF-IDF
    missing_ne = df[df['NEType'].isna()]
    if not missing_ne.empty:
        log.info(f"Step 7 – {len(missing_ne)} missing NEType rows")
        _save(missing_ne[['TICKETID','RFODescription','DESCRIPTION','Fault Type']], 'missing_ne_type_tickets.csv')
        df, _ = infer_missing_values_tfidf(df, target_col='NEType', unknown_fill_value='UNKNOWN', enable_logging=True)
        generated_ne = df[df.index.isin(missing_ne.index)][['TICKETID','NEType','RFODescription','DESCRIPTION']]
        if not generated_ne.empty:
            _save(generated_ne, 'generated_ne_type_tickets.csv')
    else:
        log.info("Step 7 – No missing NEType.")

    # Step 8 – NE_Category from NEType
    df['NE_Category'] = df['NEType'].map(lambda x: _NE_TYPE_LOOKUP.get(str(x).upper().strip(), 'Other/Unknown')
                                         if pd.notna(x) else 'Other/Unknown')
    log.info(f"Step 8 – NE_Category: {df['NE_Category'].value_counts().to_dict()}")

    # Step 9 – Clean text fields
    for col in ['RFODescription', 'RootCause', 'ActionTaken']:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
    log.info("Step 9 – Cleaned RFODescription, RootCause, ActionTaken.")

    # Step 10 – Fill blank RFODescription from RootCause
    mask = df['RFODescription'].isna()
    df.loc[mask, 'RFODescription'] = df.loc[mask, 'RootCause']
    log.info(f"Step 10 – Filled {mask.sum()} RFODescription from RootCause.")

    # Step 11 – Fill remaining blanks from ActionTaken
    mask = df['RFODescription'].isna()
    if mask.any():
        spliced = mask & df['ActionTaken'].str.contains('SPLICED', na=False, case=False)
        df.loc[spliced & (df['Urgency'] == 1), 'RFODescription'] = 'FOC CUT - LINEAR'
        df.loc[spliced & (df['Urgency'] != 1), 'RFODescription'] = 'FOC CUT WITH REDUNDANCY'
        for action, rfo in [
            ('SELF RESTORED COMMERCIAL POWER RESUMED', 'FACILITIES-Power Failure'),
            ('REPLACED',        'EQUIPMENT-Defective Hardware'),
            ('HARDWARE RESET',  'EQUIPMENT-Defective Hardware'),
            ('RECONFIGURED',    'EQUIPMENT-Configuration Problem'),
        ]:
            idx = mask & df['ActionTaken'].str.contains(action, na=False, case=False)
            df.loc[idx, 'RFODescription'] = rfo
        log.info(f"Step 11 – Inferred RFODescription from ActionTaken.")

    # Step 12 – TF-IDF on DESCRIPTION for remaining blanks
    df, inferred_rfo_log = infer_missing_values_tfidf(
        df, target_col='RFODescription', unknown_fill_value='UNKNOWN-Under Investigation', enable_logging=True
    )
    log.info("Step 12 – TF-IDF RFODescription inference complete.")

    # Step 13 – First-pass drop of Invalid / Uncategorized
    df = _filter_and_log(df, df['RFODescription'] != 'Invalid',       "Step 13a – drop RFODescription=Invalid")
    df = _filter_and_log(df, df['RFODescription'] != 'Uncategorized', "Step 13b – drop RFODescription=Uncategorized")

    # Step 14 – Standardize RFODescription
    df['Original_RFO']   = df['RFODescription']
    df['Standardized RFO'] = df.apply(standardize_rfo_description, axis=1)

    reclassified = df[
        (df['Original_RFO'].str.lower() == 'unknown under investigation') &
        (df['Standardized RFO'] != 'UNKNOWN-Under Investigation')
    ]
    if not reclassified.empty:
        _save(reclassified[['TICKETID','Original_RFO','Standardized RFO','RootCause','ActionTaken','Urgency','NEType']],
              'reclassified_unknown_rfo.csv')

    invalid_std = df[~df['Standardized RFO'].isin(EXPECTED_RFO_VALUES)]
    if not invalid_std.empty:
        _save(invalid_std[['TICKETID','RFODescription','RootCause','ActionTaken','Urgency','NEType']],
              'invalid_rfo_values.csv')
        df.loc[~df['Standardized RFO'].isin(EXPECTED_RFO_VALUES), 'Standardized RFO'] = 'Uncategorized'

    # FOC / UNKNOWN audit exports
    foc = df[df['Standardized RFO'].isin(['FOC CUT - LINEAR','FOC CUT WITH REDUNDANCY','TRANSMISSION-Fiber Problem'])]
    if not foc.empty:
        _save(foc[['TICKETID','RFODescription','Urgency','Standardized RFO']], 'foc_categorization_log.csv')

    unk = df[df['Standardized RFO'] == 'UNKNOWN-Under Investigation']
    if not unk.empty:
        unk_rfo_log = unk[['TICKETID','RFODescription','RootCause','ActionTaken','Urgency','NEType']].copy()
        if not inferred_rfo_log.empty and 'TICKETID' in inferred_rfo_log.columns:
            unk_rfo_log = unk_rfo_log.merge(inferred_rfo_log[['TICKETID','Similarity_Score']], on='TICKETID', how='left')
        _save(unk_rfo_log, 'unknown_rfo_analysis.csv')
    uncategorized_count = (df['Standardized RFO'] == 'Uncategorized').sum()
    if uncategorized_count:
        _save(df[df['Standardized RFO'] == 'Uncategorized']
              [['TICKETID','RFODescription','RootCause','ActionTaken','Urgency','NEType']],
              'uncategorized_rfo_descriptions.csv')
        log.warning(f"  {uncategorized_count} Uncategorized RFO entries")

    log.info(f"Step 14 – RFO distribution:\n{df['Standardized RFO'].value_counts(dropna=False).head(10)}")

    # Step 15 – Second-pass drop
    df = _filter_and_log(df, df['Standardized RFO'] != 'Invalid',       "Step 15a – drop Standardized RFO=Invalid")
    df = _filter_and_log(df, df['Standardized RFO'] != 'Uncategorized', "Step 15b – drop Standardized RFO=Uncategorized")

    # ── PHASE 3: GEOGRAPHIC & SITE ENRICHMENT ────────────────────────────────
    _log_phase("PHASE 3 – GEOGRAPHIC & SITE ENRICHMENT")

    # Step 16 – Assign Region
    df['Region'] = df.apply(assign_real_area, axis=1)
    log.info(f"Step 16 – Region distribution:\n{df['Region'].value_counts(dropna=False)}")
    df = _filter_and_log(df,
        df['Region'].isin(['Region 1','Region 2','Region 3','Region 4','Region 5']),
        "Step 16b – drop Unknown Area")

    # Step 17 – Merge site DB for Region 3 (ZONE / CITY)
    df_site   = load_site_database()
    r3        = df[df['Region'] == 'Region 3'].copy()
    other     = df[df['Region'] != 'Region 3'].copy()
    other[['AssignArea','AssignCity']] = 'Unknown'

    if not r3.empty:
        r3 = r3.merge(df_site, how='left', left_on='Area', right_on='PLAID', validate='many_to_many')
        unmatched = r3[r3['AssignArea'].isna()][['TICKETID','Area','Region']].drop_duplicates()
        if not unmatched.empty:
            _save(unmatched, 'unmatched_areas_region3.csv')
        r3[['AssignArea','AssignCity']] = r3[['AssignArea','AssignCity']].fillna('Unknown')
        _save(r3[['TICKETID','Area','Region','AssignArea','AssignCity']], 'region3_after_merge.csv')
        r3 = r3.drop(columns=['PLAID'], errors='ignore')
    else:
        r3 = pd.DataFrame(columns=df.columns)
        r3[['AssignArea','AssignCity']] = 'Unknown'

    df = pd.concat([r3, other], ignore_index=True)
    log.info(f"Step 17 – Rows after site merge: {len(df):,}")

    # Step 18 – Anonymize ContactGroup and Area
    _hash = lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:10] if pd.notna(x) else 'Unknown'
    df['ContactGroup'] = df['ContactGroup'].apply(_hash)
    df['Area']         = df['Area'].apply(_hash)
    log.info("Step 18 – ContactGroup and Area anonymized.")
    
    # ── PHASE 4: OPERATIONAL MODELING ────────────────────────────────────────
    _log_phase("PHASE 4 – OPERATIONAL MODELING")

    # Step 19 – Priority_Urgency
    df['Priority_Urgency'] = df['Priority'].astype(int).astype(str) + '.' + df['Urgency'].astype(int).astype(str)
    df['Priority'] = df['Priority'].astype('int8')
    df['Urgency']  = df['Urgency'].astype('int8')
    log.info(f"Step 19 – Priority_Urgency: {sorted(df['Priority_Urgency'].unique())}")

    # Step 20 – Time-based metrics & SLA
    df['REPORTDATE']   = pd.to_datetime(df['REPORTDATE'],   errors='coerce')
    df['StartDateTime']= pd.to_datetime(df['StartDateTime'],errors='coerce')
    df['OUTAGEDURATION']= pd.to_numeric(df['OUTAGEDURATION'], errors='coerce').astype('float32')
    df['RESOLVEDDATE'] = df['REPORTDATE'] + pd.to_timedelta(df['OUTAGEDURATION'], unit='h')
    df['RESOLUTION_TIME_HOURS'] = df['OUTAGEDURATION']

    fill = df['StartDateTime'].isna() & df['RESOLVEDDATE'].notna() & df['OUTAGEDURATION'].notna()
    df.loc[fill, 'StartDateTime'] = df.loc[fill, 'RESOLVEDDATE'] - pd.to_timedelta(df.loc[fill, 'OUTAGEDURATION'], unit='h')
    log.info(f"Step 20 – Filled {fill.sum()} blank StartDateTime values.")

    df['SLA_Compliant']       = df.apply(determine_sla_compliance, axis=1)
    df['DISPATCH_DELAY_HOURS']= (df['StartDateTime'] - df['REPORTDATE']).dt.total_seconds() / 3600
    df['SLA_Compliance_Rate'] = df['SLA_Compliant'] * 100
    df['SLA_Breach_Rate']     = 100 - df['SLA_Compliance_Rate']

    # Step 20b – FIELD_TIME_HOURS and Timestamp_Integrity
    # FIELD_TIME_HOURS = total MTTR minus NOC/ROC phase
    #   = OUTAGEDURATION - DISPATCH_DELAY_HOURS
    # Timestamp_Integrity = True when both phase times are non-negative
    #   and DISPATCH_DELAY_HOURS does not exceed total OUTAGEDURATION.
    # Tickets flagging False are retained in the dataset but excluded
    # from NOC/field time aggregations (data entry artefact: engineers
    # logging dispatch after ticket closure in the source system).
    df['FIELD_TIME_HOURS'] = (df['OUTAGEDURATION'] - df['DISPATCH_DELAY_HOURS']).astype('float32')
    df['Timestamp_Integrity'] = (
        df['DISPATCH_DELAY_HOURS'].notna() &
        df['FIELD_TIME_HOURS'].notna() &
        (df['FIELD_TIME_HOURS']      >= 0) &
        (df['DISPATCH_DELAY_HOURS']  >= 0) &
        (df['DISPATCH_DELAY_HOURS']  <= df['OUTAGEDURATION'])
    )
    _ti_bad = (~df['Timestamp_Integrity']).sum()
    log.info(
        f"Step 20b – FIELD_TIME_HOURS computed. "
        f"Timestamp_Integrity: {len(df)-_ti_bad:,} clean, {_ti_bad:,} flagged "
        f"({_ti_bad/len(df)*100:.1f}%)"
    )

    # Step 21 – Standardize ownership fields (blank → NaN)
    _blanks = ['', ' ', 'N/A', 'NA', 'None', 'NONE', 'null', 'NULL']
    for col in ['FT_Owner', 'WOLeadName']:
        if col in df.columns:
            df[col] = df[col].replace(_blanks, np.nan)
    log.info(f"Step 21 – FT_Owner nulls={df['FT_Owner'].isna().sum() if 'FT_Owner' in df.columns else 'N/A'}, "
                 f"WOLeadName nulls={df['WOLeadName'].isna().sum() if 'WOLeadName' in df.columns else 'N/A'}")

    # Step 22 – Resolution_Path
    def _resolution_path(row):
        if pd.isna(row.get('FT_Owner')):    return 'Auto_Self_Restored'
        if pd.isna(row.get('WOLeadName')):  return 'NOC_Remote_Restored'
        return 'Field_Dispatch_Restored'

    df['Resolution_Path'] = df.apply(_resolution_path, axis=1)
    log.info(f"Step 22 – Resolution_Path: {df['Resolution_Path'].value_counts().to_dict()}")

    rp_analysis = df.groupby('Resolution_Path').agg(
        Count=('TICKETID','count'), Avg_Duration=('OUTAGEDURATION','mean'),
        SLA_Rate=('SLA_Compliant','mean'), Avg_Dispatch=('DISPATCH_DELAY_HOURS','mean')
    ).round(2)
    _save(rp_analysis.reset_index(), 'resolution_path_analysis.csv')

    # Step 23 – Normalize Issuer Team
    def _issuer_team(x):
        if pd.isna(x): return 'Unknown'
        s = str(x).upper()
        # ROC is folded into NOC — both are network operations centre functions
        if any(p in s for p in ['NOC','ROC','NETWORK OPERATIONS','OPS']): return 'NOC'
        if any(p in s for p in ['FIELD','FO_','FO-']):              return 'Field Operations'
        if any(p in s for p in ['CARE','SUPPORT','HELPDESK']):      return 'Customer Support'
        return 'Other'

    df['Issuer_Team_Normalized'] = df['Issuer Team'].apply(_issuer_team) if 'Issuer Team' in df.columns else 'Unknown'
    log.info(f"Step 23 – Issuer_Team: {df['Issuer_Team_Normalized'].value_counts().to_dict()}")

    # Step 23b – Normalise WOOwnerGroup to privacy-safe functional categories
    # WOOwnerGroup is a team/department name (not an individual), so we normalise
    # to categories rather than hash — preserves analytical value without exposing
    # real team names. Consistent with Step 23 Issuer_Team approach.
    # WOOwnerGroup exact-match lookup (181-entry authoritative mapping).
    # ROC classified under NOC. Prefix fallback handles unseen groups.
    _WO_EXACT = {
        # Field Operations
        'FO_GMA':'Field Operations','FO_MIN':'Field Operations',
        'FO_SLZ':'Field Operations','FO_VIS':'Field Operations','FO_NLZ':'Field Operations',
        'FO OPS MIN':'Field Operations','FO OPS NCR':'Field Operations',
        'FO OPS NLZ':'Field Operations','FO OPS SLZ':'Field Operations',
        'FO OPS VIS':'Field Operations',
        'FOGMA':'Field Operations','FOSLZ':'Field Operations',
        'OPS_VIS':'Field Operations','OPS_NLZ':'Field Operations',
        'OPS_MIN':'Field Operations','OPS_SLZ':'Field Operations',
        'AREA1GMA':'Field Operations','AREA2GMA':'Field Operations',
        'AREA4GMA':'Field Operations','AREA5GMA':'Field Operations',
        'AREA6GMA':'Field Operations',
        # NOC (includes ROC)
        'ROC':'NOC','ROC GMA':'NOC','ROC NLZ':'NOC','ROC VIS':'NOC',
        'NOC-WLN':'NOC','NOC-TRS':'NOC','NOC-IPRAN':'NOC','NOC-RAN':'NOC',
        'NOC-IPCORE':'NOC','NOC-VAS':'NOC','NOC-CSCORE':'NOC',
        'NOC_ROAMNG':'NOC','NOC_WLS':'NOC',
        'OSS':'NOC','EDSC':'NOC','RSCNFEMIN':'NOC','TRS_NOC':'NOC',
        'SCC TAC':'NOC','GTNOCTRS':'NOC',
        # Fiber Restoration Team
        'CNO':'Fiber Restoration','CNO GMA':'Fiber Restoration',
        'CNO MIN':'Fiber Restoration','CNO NLZ':'Fiber Restoration',
        'CNO SLZ':'Fiber Restoration','CNO VIS':'Fiber Restoration',
        'CNOGMA':'Fiber Restoration','CNOMIN':'Fiber Restoration',
        'CNONLZ':'Fiber Restoration','CNOVIS':'Fiber Restoration','OSP':'Fiber Restoration',
        # Core Operations
        'HEO':'Core Operations','HEO_GMA':'Core Operations','HEO_SLZ':'Core Operations',
        'HEO_VIS':'Core Operations','HEO_NLZ':'Core Operations','HEO_MIN':'Core Operations',
        'HEO_CRM':'Core Operations','HEO_ICS':'Core Operations',
        'RMF_GMA':'Core Operations','RMF_SLZ':'Core Operations','RMF_VIS':'Core Operations',
        'RMF_NLZ':'Core Operations','RMF_MIN':'Core Operations',
        'RMFGMA':'Core Operations','RMFSLZ':'Core Operations','RMFVIS':'Core Operations',
        'PGRP028':'Core Operations','PGRP029':'Core Operations','PGRP033':'Core Operations',
        'PGRP034':'Core Operations','PGRP035':'Core Operations','PGRP036':'Core Operations',
        'PGRP037':'Core Operations','PGRP038':'Core Operations','PGRP039':'Core Operations',
        'PGRP040':'Core Operations','PGRP041':'Core Operations','PGRP042':'Core Operations',
        'PGRP043':'Core Operations','PGRP044':'Core Operations','PGRP046':'Core Operations',
        'PGRP047':'Core Operations','PGRP055':'Core Operations','PGRP072':'Core Operations',
        'PGRP080':'Core Operations','PGRP095':'Core Operations',
        'CNFM':'Core Operations','CNFM NCR':'Core Operations','CNFM VIS':'Core Operations',
        'CNFM SLZ':'Core Operations','CNFM NLZ':'Core Operations','CNFM MIN':'Core Operations',
        'EITSC-CNFM':'Core Operations',
        'ECMPGRP003':'Core Operations','ECMPGRP004':'Core Operations',
        'ECMPGRP005':'Core Operations','ECMPGRP006':'Core Operations',
        'ECMPGRP008':'Core Operations','ECMPGRP021':'Core Operations',
        'NCRFGRP019':'Core Operations','NCRFGRP024':'Core Operations',
        'NCRFGRP025':'Core Operations','NCRFGRP052':'Core Operations',
        'NCRFGRP054':'Core Operations','NCRFGRP060':'Core Operations',
        'NCRFGRP069':'Core Operations','NCRFGRP077':'Core Operations',
        'NCRFGRP080':'Core Operations',
        'SDATIER1':'Core Operations','AMMINZMB':'Core Operations',
        'AMMINDVO':'Core Operations','GTCOSS':'Core Operations',
        # Engineering
        'IBSPD':'Engineering','IBS_RETRO':'Engineering','IBS NCR':'Engineering',
        'IBS NONNCR':'Engineering',
        'WLSPD NCR':'Engineering','WLSPD_ENCR':'Engineering','WLSPD SLZ':'Engineering',
        'WLSPD NLZ':'Engineering','WLSPD VIS':'Engineering','WLSPD MIN':'Engineering',
        'WLSPD_ENLZ':'Engineering','WLSPD_ESLZ':'Engineering','WLSPD_EMIN':'Engineering',
        'RGPM NCR':'Engineering','RGPM MIN':'Engineering','RGPM SLZ':'Engineering',
        'RGPM NLZ':'Engineering','RGPM-DS-NC':'Engineering',
        'SWPR NCR':'Engineering','PMO':'Engineering','NPD':'Engineering',
        'TND_TNI':'Engineering','TND_TNO':'Engineering',
        'TRIX010':'Engineering','TRIX016':'Engineering','TRIX017':'Engineering',
        'TRIX026':'Engineering','TRIX028':'Engineering',
        'TRIX030':'Engineering','TRIX031':'Engineering',
        'TRS_TNINCR':'Engineering',
        'AEPM_REPM':'Engineering','AEPM_FACIL':'Engineering','AEPM_RGPM':'Engineering',
        'AEPM_WCNFE':'Engineering','AEPM NCR':'Engineering',
        'FTTHPD NCR':'Engineering','FTTHPD LZN':'Engineering',
        'SLCKRRENLZ':'Engineering','SLCKRRESLZ':'Engineering',
        'SQUAD_SL1':'Engineering','PROP TEO':'Engineering','PRP PD NCR':'Engineering',
        # Tier 2 Support
        '1211':'Tier 2 Support','TRD':'Tier 2 Support','TRDGMA':'Tier 2 Support',
        'TRD-AC-NCR':'Tier 2 Support','TRD-AC-NLZ':'Tier 2 Support',
        'TRD-AC-VIS':'Tier 2 Support','TRD-AC-SLZ':'Tier 2 Support',
        'TRD-AC-MIN':'Tier 2 Support',
        'TRD-COR-FX':'Tier 2 Support','TRD-COR-TR':'Tier 2 Support',
        'TRD-COR-CS':'Tier 2 Support',
        'TRD-IPNO':'Tier 2 Support','TRD-SPM':'Tier 2 Support',
        'TIER2_SDN':'Tier 2 Support','CUS_TIER2':'Tier 2 Support',
        'CUS VISMIN':'Tier 2 Support',
        # Facilities Operations
        'FSGMA':'Facilities Operations','FACGMA':'Facilities Operations',
        'CHGSO2APP':'Facilities Operations','EITSC_FM':'Facilities Operations',
        'CFS GMA':'Facilities Operations','BFS_FMS':'Facilities Operations',
        # Niche
        'ECOPS_GMA':'Enterprise Operations',
        'RSOGMA':'Operations Support',
        'NSP_MDTS':'Third Party',
    }
    _WO_PREFIX = [
        (['NSP_','NSP-'],                                   'Third Party'),
        (['EITSC_FM','EITSC-FM','BFS_','CFS ','FACGMA',
          'FSGMA','CHGSO'],                                 'Facilities Operations'),
        (['CNO'],                                           'Fiber Restoration'),
        (['NOC-','NOC_','ROC ','ROC_','ROC-'],              'NOC'),
        (['TRS_NOC','RSCNFE','SCC ','GTNOC'],               'NOC'),
        (['HEO','RMF_','RMFG','RMFS','RMFV','CNFM',
          'PGRP','ECMPGRP','NCRFGRP','EITSC','SDATIER',
          'AMMIN','GTCOS'],                                 'Core Operations'),
        (['TRD-','TRD ','TRDG','TIER2','CUS_','CUS '],      'Tier 2 Support'),
        (['IBS','WLSPD','RGPM','SWPR','PMO','TND_',
          'TRIX','TRS_T','AEPM','FTTHPD','SLCKR',
          'SQUAD','PROP ','PRP ','NPD'],                    'Engineering'),
        (['FO_','FO ','FOSLZ','FOGMA','OPS_','AREA'],       'Field Operations'),
        (['ECOPS'],                                         'Enterprise Operations'),
        (['RSO'],                                           'Operations Support'),
    ]

    def _wo_owner_category(x):
        """Normalise WOOwnerGroup to privacy-safe functional category.
        Exact-match on 181-entry authoritative map, then prefix fallback.
        ROC under NOC. Categories: Field Operations, NOC, Core Operations,
        Engineering, Fiber Restoration, Tier 2 Support, Facilities Operations,
        Enterprise Operations, Operations Support, Third Party, Other, Unknown."""
        if pd.isna(x):
            return 'Unknown'
        s_raw = str(x).strip()
        if s_raw in ('', 'N/A', 'NA', 'None', 'NONE', 'null', 'NULL'):
            return 'Unknown'
        if s_raw in _WO_EXACT:
            return _WO_EXACT[s_raw]
        s = s_raw.upper()
        for prefixes, cat in _WO_PREFIX:
            if any(s.startswith(p.upper()) for p in prefixes):
                return cat
        return 'Other'


    if 'WOOwnerGroup' in df.columns:
        df['WOOwnerGroup_Category'] = df['WOOwnerGroup'].apply(_wo_owner_category)
        log.info(
            f"Step 23b – WOOwnerGroup_Category: "
            f"{df['WOOwnerGroup_Category'].value_counts().to_dict()}"
        )
    else:
        df['WOOwnerGroup_Category'] = 'Unknown'
        log.warning(
            "Step 23b – WOOwnerGroup column not found in raw data; "
            "WOOwnerGroup_Category set to 'Unknown'"
        )

    # Step 24 – SiteName
    df['SiteName'] = (df['Region'].str.replace(' ','') + '_' +
                      df['AssignCity'].fillna('Unknown') + '_' +
                      df['Area'].str[:8])
    log.info(f"Step 24 – {df['SiteName'].nunique()} unique SiteNames")

    site_stats = df.groupby('SiteName').agg(
        Tickets=('TICKETID','count'), SLA_Rate=('SLA_Compliant','mean'),
        Avg_Duration=('OUTAGEDURATION','mean'), Region=('Region','first'), City=('AssignCity','first')
    ).round(2).sort_values('Tickets', ascending=False)
    _save(site_stats.head(100).reset_index(), 'top_100_sites.csv')

    if region_scope:
        df = df[df["Region"] == region_scope].copy()    
    
    # ── PHASE 5: PRIVACY & OUTPUT PROTECTION ─────────────────────────────────
    _log_phase("PHASE 5 – PRIVACY & OUTPUT PROTECTION")

    # Step 25 – Anonymize personnel
    def _anon(name):
        if pd.isna(name) or name == 'Unknown': return 'Unknown'
        return 'ENG_' + hashlib.sha256(str(name).encode()).hexdigest()[:8]

    for raw_col, anon_col, map_file in [
        ('FT_Owner',   'FT_Owner_Anonymized',   'ft_owner_anonymization_map.csv'),
        ('WOLeadName', 'WOLeadName_Anonymized',  'wo_lead_anonymization_map.csv'),
    ]:
        if raw_col in df.columns:
            df[anon_col] = df[raw_col].apply(_anon)
            _save(df[[raw_col, anon_col]].drop_duplicates(), map_file)
            log.info(f"  {raw_col} → {df[raw_col].nunique()} names → {df[anon_col].nunique()} codes")
        else:
            df[anon_col] = 'Unknown'

    # Step 26 – Drop raw personnel columns
    drop_cols = ['FT_OwnerID','FT_reportedById','WOOwnerID','WOLeadID','CHANGEBY',
                 'FT_Owner','WOLeadName','FT_ReportedByName','Cleared By','Assigned To','SUPERVISOR',
                 'WOOwnerGroup']  # WOOwnerGroup replaced by WOOwnerGroup_Category
    dropped = [c for c in drop_cols if c in df.columns]
    df = df.drop(columns=dropped, errors='ignore')
    log.info(f"Step 26 – Dropped {len(dropped)} personnel columns: {dropped}")

    # ── FINAL: SELECT COLUMNS, VALIDATE, LOG ──────────────────────────────────
    final_columns = {
        'TICKETID': 'TICKETID', 'REPORTDATE': 'REPORTDATE',
        'Priority': 'Priority', 'Urgency': 'Urgency', 'Priority_Urgency': 'Priority_Urgency',
        'OUTAGEDURATION': 'OUTAGEDURATION', 'ContactGroup': 'ContactGroup', 'Area': 'Area',
        'StartDateTime': 'DISPATCHDATE', 'NEType': 'NEType', 'NE_Category': 'NE_Category',
        'Region': 'Region', 'Standardized RFO': 'Standardized RFO',
        'AssignArea': 'ZONE', 'AssignCity': 'CITY',
        'RESOLVEDDATE': 'RESOLVEDDATE', 'RESOLUTION_TIME_HOURS': 'RESOLUTION_TIME_HOURS',
        'SLA_Compliant': 'SLA_Compliant', 'DISPATCH_DELAY_HOURS': 'DISPATCH_DELAY_HOURS',
        'SLA_Compliance_Rate': 'SLA_Compliance_Rate', 'SLA_Breach_Rate': 'SLA_Breach_Rate',
        'Resolution_Path': 'Resolution_Path', 'Issuer_Team_Normalized': 'Issuer_Team',
        'SiteName': 'SiteName',
        'FIELD_TIME_HOURS': 'FIELD_TIME_HOURS',
        'Timestamp_Integrity': 'Timestamp_Integrity',
        'FT_Owner_Anonymized': 'Engineer_ID', 'WOLeadName_Anonymized': 'Field_Lead_ID',
        'WOOwnerGroup_Category': 'WOOwnerGroup',  # normalised team category, privacy-safe
    }
    avail = {k: v for k, v in final_columns.items() if k in df.columns}
    df    = df[list(avail.keys())].rename(columns=avail)

    _log_phase("POST-CLEAN VALIDATION")
    validate_cleaned_data(df)

    elapsed  = time.time() - start
    retained = len(df) / initial_rows * 100
    log.info(f"\nPipeline complete │ {initial_rows:,} → {len(df):,} rows │ "
                 f"retained {retained:.1f}% │ {elapsed:.1f}s │ {len(df)/elapsed:.0f} rows/s")
    if save_output:
        _save(df, 'cleaned_fault_ticket.csv')
    
    return df
