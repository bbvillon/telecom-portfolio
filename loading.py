"""
Data Loading Module for Telecom Fault Ticket Analysis
Handles CSV loading with validation and site database integration.
"""

import pandas as pd
import numpy as np
import os
import glob
import logging
import geopandas as gpd
from config import output_folder


def load_data(file_path='../data/fault_tickets.csv'):
    """
    Load fault ticket CSV with validation. Raises error if file not found or columns missing.
    
    Parameters:
        file_path (str): Path to fault tickets CSV (tab-delimited)
        
    Returns:
        pd.DataFrame: Validated dataframe with all required columns
        
    Raises:
        FileNotFoundError: If CSV not found
        ValueError: If required columns missing
    """
    required_columns = [
        'TICKETID', 'REPORTDATE', 'FT Status', 'Priority', 'Urgency', 
        'OUTAGEDURATION', 'RFODescription', 'ContactGroup', 'Area', 
        'Fault Type', 'DESCRIPTION', 'RootCause', 'StartDateTime', 
        'NEType', 'ActionTaken'
    ]
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Fault ticket file not found: {file_path}\n"
            f"Please provide a valid CSV file path or generate synthetic data externally."
        )
    
    try:
        df = pd.read_csv(
            file_path,
            sep='\t',
            on_bad_lines='warn',
            low_memory=False,
            encoding='utf-8',
            skipinitialspace=True,
            index_col=False
        )
        logging.info(f"Loaded {len(df):,} rows from {file_path}")
        
        # Validate columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        return df
        
    except pd.errors.ParserError as e:
        raise ValueError(f"CSV parsing error in {file_path}: {e}")


def load_site_database(file_path='./data/external/site_database.csv'):
    """
    Load site database CSV for Region 3 enrichment (PLAID → Zone/City mapping).
    
    Parameters:
        file_path (str): Path to site database CSV
        
    Returns:
        pd.DataFrame: Site data with PLAID, AssignArea, AssignCity columns
        
    Raises:
        FileNotFoundError: If CSV not found
        ValueError: If required columns missing
    """
    required_cols = ['PLAID', 'AssignArea', 'AssignCity']
    
    if not os.path.exists(file_path):
        logging.warning(
            f"Site database not found: {file_path}\n"
            f"Region 3 tickets will have Zone/City set to 'Unknown'."
        )
        # Return empty dataframe with correct schema so merge doesn't break
        return pd.DataFrame(columns=required_cols)
    
    try:
        df_site = pd.read_csv(file_path, low_memory=False, encoding='utf-8')
        logging.info(f"Loaded site database: {len(df_site):,} rows")
        
        # Validate columns
        missing_cols = [col for col in required_cols if col not in df_site.columns]
        if missing_cols:
            raise ValueError(f"Site database missing required columns: {missing_cols}")
        
        # Keep only required columns
        df_site = df_site[required_cols]
        
        # Handle duplicates
        dupes = df_site['PLAID'].duplicated(keep='first')
        if dupes.any():
            logging.warning(f"Found {dupes.sum()} duplicate PLAID values - keeping first occurrence")
            df_site = df_site[~dupes]
        
        # Standardize PLAID (uppercase, stripped)
        df_site['PLAID'] = df_site['PLAID'].astype(str).str.strip().str.upper()
        
        return df_site
        
    except pd.errors.ParserError as e:
        raise ValueError(f"CSV parsing error in {file_path}: {e}")


def load_and_process_geojson(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Load Philippine region GeoJSON files and merge with ticket volume for map visualizations.
    
    Parameters:
        df (pd.DataFrame): Cleaned ticket data with 'Region' column
        
    Returns:
        gpd.GeoDataFrame: Region geometries with ticket volumes, or empty GDF if files not found
        
    Raises:
        KeyError: If GeoJSON files missing expected columns (adm2_en, adm1_psgc)
    """
    geojson_dir = '../data/external/geojson/regions/medres'
    geojson_pattern = os.path.join(geojson_dir, 'ph_region_*.json')
    geojson_files = glob.glob(geojson_pattern)
    
    if not geojson_files:
        logging.warning(
            f"No GeoJSON files found in {geojson_dir}\n"
            f"Map visualizations will use bar chart fallback."
        )
        return gpd.GeoDataFrame()
    
    gdfs = []
    for file in geojson_files:
        try:
            gdf_temp = gpd.read_file(file)
            
            if 'geometry' not in gdf_temp.columns:
                logging.warning(f"Skipping {file} - no geometry column")
                continue
            
            # Extract region number from filename (ph_region_01.json → 1)
            region_num = int(os.path.basename(file).split('_')[2].split('.')[0].lstrip('0') or 0)
            gdf_temp['Region_Num'] = region_num
            gdfs.append(gdf_temp)
            
        except Exception as e:
            logging.warning(f"Error loading {file}: {e}")
    
    if not gdfs:
        logging.warning("No valid GeoJSON files loaded - using bar chart fallback")
        return gpd.GeoDataFrame()
    
    # Concatenate all regions
    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    
    # Validate required columns
    if 'adm2_en' not in gdf.columns or 'adm1_psgc' not in gdf.columns:
        raise KeyError("GeoJSON missing expected columns: 'adm2_en' or 'adm1_psgc'")
    
    # Map PSGC codes to region numbers (Philippine Standard Geographic Code)
    psgc_to_region = {
        '100000000': 1,  '200000000': 2,  '300000000': 3,  '400000000': 4,
        '500000000': 5,  '600000000': 6,  '700000000': 7,  '800000000': 8,
        '900000000': 9,  '1000000000': 10, '1100000000': 11, '1200000000': 12,
        '1300000000': 13, '1400000000': 14, '1600000000': 15, '1700000000': 16,
        '1900000000': 17
    }
    
    gdf['Corrected_Region_Num'] = gdf['adm1_psgc'].map(psgc_to_region).fillna(gdf['Region_Num'])
    
    # Group Philippine regions into 5 telecom operational regions
    region_grouping = {
        'Region 1': [1, 2, 14, 3],      # North Luzon + CAR
        'Region 2': [4, 5, 16],         # South Luzon
        'Region 3': [13],               # NCR (Metro Manila)
        'Region 4': [6, 7, 8],          # Visayas
        'Region 5': [9, 10, 11, 12, 15, 17]  # Mindanao
    }
    
    gdf['Region Name'] = gdf['Corrected_Region_Num'].apply(
        lambda x: next((k for k, v in region_grouping.items() if x in v), None)
    )
    
    # Dissolve geometries by region name (combine overlapping areas)
    gdf = gdf.dissolve(by='Region Name').reset_index()
    
    if 'Region Name' not in gdf.columns:
        raise KeyError("Region Name missing after dissolve - check region_grouping logic")
    
    # Calculate ticket volume per region
    ticket_volume = df.groupby('Region').size().reset_index(name='Ticket Volume')
    
    # Standardize region names for merge
    region_mapping = {
        'Region 1': 'Region 1',
        'Region 2': 'Region 2',
        'Region 3': 'Region 3',
        'Region 4': 'Region 4',
        'Region 5': 'Region 5'
    }
    ticket_volume['Region Name'] = ticket_volume['Region'].map(region_mapping).fillna(
        ticket_volume['Region'].astype(str)
    )
    
    # Merge ticket volume with geometries
    gdf = gdf.merge(ticket_volume, on='Region Name', how='left').fillna({'Ticket Volume': 0})
    
    return gdf
