"""
Central Configuration for Telecom Fault Ticket Analysis
All constants, paths, and shared configuration in one place.
"""

import os
import logging
import logging.handlers
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════
# PATHS & DIRECTORIES
# ═══════════════════════════════════════════════════════════════════════════

output_folder = 'output'
data_folder = '../data'
geojson_folder = os.path.join(data_folder, 'ph_maps/2023/geojson/regions/medres')


# ═══════════════════════════════════════════════════════════════════════════
# SLA CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

# SLA thresholds in hours (Priority-Urgency combinations)
SLA_THRESHOLDS = {
    # Dash notation (primary)
    '1-1': 3,   '1-2': 3,   '1-3': 3,
    '2-1': 6,   '2-2': 6,  '2-3': 6,
    '3-1': 9,  '3-2': 12,  '3-3': 24,
    '4-1': 72,  '4-2': 72,  '4-3': 72,
    # Dot notation (backwards compatibility)
    '1.1': 3,   '1.2': 3,   '1.3': 3,
    '2.1': 6,   '2.2': 6,  '2.3': 6,
    '3.1': 9,  '3.2': 12,  '3.3': 24,
    '4.1': 72,  '4.2': 72,  '4.3': 72,
}

# Legacy mapping for backwards compatibility
sla_mapping = SLA_THRESHOLDS


# ═══════════════════════════════════════════════════════════════════════════
# REGION CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

REGION_ORDER = [f'Region {i}' for i in range(1, 6)]
ZONE_ORDER = [f'ZONE {i}' for i in range(1, 7)]

# Philippine PSGC (Standard Geographic Code) to operational region mapping
PSGC_TO_REGION = {
    '100000000': 1,  '200000000': 2,  '300000000': 3,  '400000000': 4,
    '500000000': 5,  '600000000': 6,  '700000000': 7,  '800000000': 8,
    '900000000': 9,  '1000000000': 10, '1100000000': 11, '1200000000': 12,
    '1300000000': 13, '1400000000': 14, '1600000000': 15, '1700000000': 16,
    '1900000000': 17
}

# 17 Philippine regions → 5 telecom operational regions
REGION_GROUPING = {
    'Region 1': [1, 2, 14, 3],      # North Luzon + CAR
    'Region 2': [4, 5, 16],         # South Luzon
    'Region 3': [13],               # NCR (Metro Manila)
    'Region 4': [6, 7, 8],          # Visayas
    'Region 5': [9, 10, 11, 12, 15, 17]  # Mindanao
}


# ═══════════════════════════════════════════════════════════════════════════
# RFO (ROOT CAUSE) CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

EXPECTED_RFO_VALUES = [
    'FOC CUT - LINEAR',
    'FOC CUT WITH REDUNDANCY',
    'ADMIN-Lessor Related Cause',
    'EQUIPMENT-Configuration Problem',
    'EQUIPMENT-Decommissioned',
    'EQUIPMENT-Defective Hardware',
    'EQUIPMENT-Software Problem',
    'FACILITIES-Cooling System Problem',
    'FACILITIES-Design Problem',
    'FACILITIES-Monitoring Equipment Problem',
    'FACILITIES-Power Failure',
    'FACILITIES-Water Seepage',
    'OTHERS-Force Majeure',
    'OTHERS-Preventive Maintenance',
    'OTHERS-Security Breach',
    'THIRD PARTY-Activity Related',
    'THIRD PARTY-POI',
    'TRANSMISSION-Cable Problem',
    'TRANSMISSION-Capacity Problem',
    'TRANSMISSION-Equipment Problem',
    'TRANSMISSION-Fiber Problem',
    'TRANSMISSION-IP Network Problem',
    'TRANSMISSION-Microwave Antenna System Problem',
    'TRANSMISSION-Offnetwork',
    'UNKNOWN-Under Investigation',
    'Invalid',
]


# ═══════════════════════════════════════════════════════════════════════════
# COLOR PALETTES
# ═══════════════════════════════════════════════════════════════════════════

# Professional color palette for general use
PROFESSIONAL_PALETTE = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
]

# RFO-specific color mapping
RFO_PALETTE = {
    'FOC CUT - LINEAR': '#d62728',
    'FOC CUT WITH REDUNDANCY': '#ff7f0e',
    'EQUIPMENT-Defective Hardware': '#1f77b4',
    'FACILITIES-Power Failure': '#9467bd',
    'TRANSMISSION-Fiber Problem': '#8c564b',
    'EQUIPMENT-Configuration Problem': '#e377c2',
    'TRANSMISSION-IP Network Problem': '#7f7f7f',
    'UNKNOWN-Under Investigation': '#bcbd22',
}

# Region-specific colors
ZONE_PALETTE = {
    'ZONE 1': '#1f77b4',
    'ZONE 2': '#ff7f0e',
    'ZONE 3': '#2ca02c',
    'ZONE 4': '#d62728',
    'ZONE 5': '#9467bd',
    'ZONE 6': '#8c564b',
}


# ═══════════════════════════════════════════════════════════════════════════
# PLOT CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

DEFAULT_DPI = 300
DEFAULT_FIGSIZE = (12, 8)
DEFAULT_ROTATION = 45
ANNOTATION_FONTSIZE = 10
TITLE_FONTSIZE = 12
LABEL_FONTSIZE = 10


# ═══════════════════════════════════════════════════════════════════════════
# VALIDATION RULES
# ═══════════════════════════════════════════════════════════════════════════

REQUIRED_COLUMNS = [
    'TICKETID', 'REPORTDATE', 'FT Status', 'Priority', 'Urgency',
    'OUTAGEDURATION', 'RFODescription', 'ContactGroup', 'Area',
    'Fault Type', 'DESCRIPTION', 'RootCause', 'StartDateTime',
    'NEType', 'ActionTaken'
]

VALID_PRIORITIES = [1, 2, 3]
VALID_URGENCIES = [1, 2, 3]
VALID_STATUSES = ['CLOSED', 'RESOLVED', 'PENRESOLVE']

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)

def get_logger(name: str = __name__) -> logging.Logger:
    """Returns a configured logger. Call this in any module instead of print()."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # avoid duplicate handlers on re-import

    logger.setLevel(logging.DEBUG)

    # Console handler — INFO and above
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter('%(levelname)s | %(name)s | %(message)s'))

    # File handler — DEBUG and above, rotating at 5MB
    fh = logging.handlers.RotatingFileHandler(
        LOG_DIR / 'pipeline.log', maxBytes=5_000_000, backupCount=3, encoding='utf-8'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger