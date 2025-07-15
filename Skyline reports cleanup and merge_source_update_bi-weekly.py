import os
import glob
import pandas as pd
import numpy as np
import re
import datetime
import shutil

folder_path = r'C:\Users\Rita.Fazlyeva\Shell\Canada Integrated Gas - DIY Non-Production Workspace - Drawing Library'

def extract_metadata(lines):
    well_name, unique_well_id, formation = '', '', ''
    for line in lines[:10]:  # Only check first 10 lines for metadata
        columns = re.split(r'[,\t]', line.strip())
        if len(columns) > 1:
            if columns[0].strip().lower() == 'well name':
                well_name = columns[1].strip()
            elif columns[0].strip().lower() == 'unique well id':
                unique_well_id = columns[1].strip()
            elif columns[0].strip().lower() == 'formation':
                formation = columns[1].strip()
        elif ':' in line:
            key, value = line.split(':', 1)
            key = key.strip().lower()
            value = value.strip()
            if key == 'well name':
                well_name = value
            elif key == 'unique well id':
                unique_well_id = value
            elif key == 'formation':
                formation = value
    return well_name, unique_well_id, formation

def fix_uwi(uwi):
    if pd.isna(uwi):
        return uwi
    uwi = str(uwi).strip()
    if re.match(r".*/\d{2}$", uwi):
        return uwi
    if re.match(r".*/\d$", uwi):
        return uwi + "0"
    if uwi.endswith('/'):
        return uwi + "00"
    return uwi + "/00"

def normalize_col(col):
    """Normalize column names by converting to lowercase and removing special characters"""
    if pd.isna(col) or col is None:
        return ""
    # Replace underscores and hyphens with spaces, then normalize multiple spaces to single space
    normalized = str(col).strip().lower().replace('_', ' ').replace('-', ' ')
    # Replace multiple spaces with single space
    return re.sub(r'\s+', ' ', normalized)

def process_files_in_folder(folder_path):
    processed_files = []
    dataframes = []
    problem_files = []  # List to collect files missing Unique Well Information
    
    # First, check and move old files to completed folder
    print("Checking for files older than 2 weeks...")
    moved_files = check_and_move_old_files(folder_path)
    
    try:
        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        excel_files = glob.glob(os.path.join(folder_path, '*.xls')) + glob.glob(os.path.join(folder_path, '*.xlsx'))
        all_files = csv_files + excel_files

        for file_path in all_files:
            try:
                if os.path.basename(file_path).startswith('processed_'):
                    print(f"Skipping already processed file: {file_path}")
                    continue

                if file_path.endswith('.csv'):
                    with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as file:
                        lines = file.readlines()
                    well_name, unique_well_id, formation = extract_metadata(lines)

                    # Flag files missing Unique Well Information
                    if not (well_name and unique_well_id and formation):
                        problem_files.append(file_path)

                    # Improved header row detection
                    header_row_index = None
                    for i, line in enumerate(lines):
                        if re.search(r'\bdate\b', line, re.IGNORECASE) and re.search(r'\btime\b', line, re.IGNORECASE):
                            header_row_index = i
                            break
                    if header_row_index is None:
                        for i, line in enumerate(lines):
                            columns = re.split(r'[,\t]', line.strip())
                            if len([col for col in columns if col]) > 5:
                                header_row_index = i
                                break

                    if header_row_index is not None:
                        column_names = re.split(r'[,\t]', lines[header_row_index].strip())
                        processed_data = [column_names]

                        for line in lines[header_row_index + 1:]:
                            row_data = re.split(r'[,\t]', line.strip())
                            if len(row_data) == len(column_names):
                                processed_data.append(row_data)

                        df = pd.DataFrame(processed_data[1:], columns=column_names)
                        if len(df) > 1:
                            df = df.drop(df.index[0]).reset_index(drop=True)
                    else:
                        print(f"No header row with more than 5 columns found in {file_path}. Skipping file.")
                        continue

                elif file_path.endswith(('.xls', '.xlsx')):
                    df = pd.read_excel(file_path, sheet_name=0)
                    well_name = df.iloc[0, 1] if isinstance(df.iloc[0, 1], str) and 'Well Name' in str(df.iloc[0, 0]) else ''
                    unique_well_id = df.iloc[1, 1] if isinstance(df.iloc[1, 1], str) and 'Unique Well ID' in str(df.iloc[1, 0]) else ''
                    formation = df.iloc[2, 1] if isinstance(df.iloc[2, 1], str) and 'Formation' in str(df.iloc[2, 0]) else ''

                    # Flag files missing Unique Well Information
                    if not (well_name and unique_well_id and formation):
                        problem_files.append(file_path)

                    df = df.iloc[3:]
                    df.columns = df.iloc[0]
                    df = df[1:]

                # Remove empty columns and comments column
                df = df.loc[:, (df.columns != "") & (df.columns.str.lower() != "comments")]

                # Check for columns that need interpolation - using case-insensitive matching
                columns_to_interpolate = [
                    'static press', 'diff press', 'meter temp', 'total gas rate',
                    'total gas produced', 'total gas flared', 'total gas pipelined', 'water gain',
                    'water cum', 'condi gain', 'condi cum', 'total fluids gain', 'ph', 'salinity',
                    'wgr', 'cgr', 'lgr', 'cum wells to floc tank', 'abraisives level flock tank (cm)',
                    'abraisives volume flock tank', 'abraisives to disposal', 'gas rate (inst)', 'bs&w',
                    'condi rate', 'water rate', 'api sample temp', 'api @60f', 
                    'lftr', 'pipeline pressure', 'pipeline temp'
                ]

                # Find matching columns (case-insensitive)
                for target_col in columns_to_interpolate:
                    matching_cols = [col for col in df.columns if col.lower().strip() == target_col.lower()]
                    for col in matching_cols:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        first_nonzero_index = df[df[col] != 0].index.min()
                        if pd.notna(first_nonzero_index):
                            mask = df.index >= first_nonzero_index
                            interpolated = df[col].copy()
                            interpolated[mask] = interpolated[mask].interpolate(method='linear', limit_direction='forward')
                            df[col] = interpolated

                # Remove duplicate rows based on volumetric totals
                vt_cols = ['total gas produced', 'condi cum', 'water cum']
                existing_vt_cols = [col for col in df.columns if col.lower().strip() in [v.lower() for v in vt_cols]]
                if existing_vt_cols:
                    df = df.loc[~(df[existing_vt_cols] == df[existing_vt_cols].shift()).all(axis=1)]

                # Handle specific columns with forward fill
                ffill_cols = ['ph', 'salinity', 'measured api', 'choke size']
                for target_col in ffill_cols:
                    matching_cols = [col for col in df.columns if col.lower().strip() == target_col.lower()]
                    for col in matching_cols:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].where(df[col] > 0, np.nan)
                        df[col] = df[col].ffill().bfill()

                # Add metadata columns if they don't exist
                well_name_cols = [col for col in df.columns if col.lower().strip() == 'well name']
                if not well_name_cols:
                    df.insert(0, 'Well Name', well_name)
                
                uwi_cols = [col for col in df.columns if 'unique well' in col.lower()]
                if not uwi_cols:
                    df.insert(1, 'Unique Well Identifier', unique_well_id)
                
                formation_cols = [col for col in df.columns if col.lower().strip() == 'formation']
                if not formation_cols:
                    df.insert(2, 'Formation', formation)

                # Add source file column
                df['Source File'] = os.path.basename(file_path)

                # Save processed file
                output_file_path = os.path.join(folder_path, 'processed_' + os.path.basename(file_path))
                if os.path.exists(output_file_path):
                    os.remove(output_file_path)
                df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
                print(f"File processed and saved to {output_file_path}")
                processed_files.append(output_file_path)
                
            except Exception as e:
                print(f"An error occurred while processing {file_path}: {e}")

        # Print and save problematic files
        if problem_files:
            print("\nFiles missing Unique Well Information:")
            for pf in problem_files:
                print(pf)
            with open(os.path.join(folder_path, 'problem_files.txt'), 'w') as f:
                for pf in problem_files:
                    f.write(pf + '\n')
        else:
            print("\nAll files have Unique Well Information.")

        # Column mapping for standardization
        column_mapping = {
            'well name': 'Well Name',  
            'choke size': 'Choke Size (mm)',
            'static press': 'Static Press (kPa)',
            'diff press': 'Diff Press (kPa)',
            'meter temp': 'Meter Temp (C)',
            'total gas produced': 'Total Gas Produced (e3m3)',
            'total gas flared': 'Total Gas Flared (e3m3)',
            'total gas pipelined': 'Total Gas Pipelined (e3m3)',
            'water gain': 'Water Gain (m3)',
            'water cum': 'Water Cum (m3)',
            'condi gain': 'Condi Gain (m3)',
            'condi cum': 'Condi Cum (m3)',
            'total fluids gain': 'Total Fluids Gain (m3)',
            'cum wells to floc tank': 'Cum wells to floc tank',
            'abraisives level flock tank (cm)': 'Abraisives Level Flock Tank (cm)',
            'abraisives volume flock tank': 'Abraisives Volume Flock Tank (m3)',
            'abraisives to disposal': 'Abraisives to Disposal (m3)',
            'gas rate (inst)': 'Gas Rate (Inst) (e3m3/d)',
            'water rate': 'Water Rate (m3/d)',
            'measured api': 'Measured API',
            'api sample temp': 'API Sample Temp',
            'sand': 'Sand(%)',
            'pipeline pressure': 'PL pres',
            'pipeline temp': 'PL temp',
            'date': 'Date',
            'time': 'Time',
            'flow time': 'Flow Time',
            'h2s': 'H2S (%, ppm)',
            'casing press': 'Casing Press (kPa)',
            'tubing press': 'Tubing Press (kPa)',
            'flow temp': 'Flow Temp (C)',
            'orifice (mm)': 'Orifice (mm)',
            'ph': 'pH',
            'salinity': 'Salinity (% or ppm)',
            'wgr': 'WGR (m3/e3m3)',
            'cgr': 'CGR (m3/e3m3)',
            'lgr': 'LGR (m3/e3m3)',
            'gor': 'GOR (e3m3/m3)',
            'bs&w': 'BS&W',
            'lftr': 'LFTR',
            'gas incin': 'Gas Incin (e3m3)',
            'gas vented': 'Gas Vented (e3m3)',
            'total oil rate': 'Total Oil Rate (m3/d)',
            'condi rate': 'Condi Rate (m3/d)',
            'total water rate': 'Total Water Rate (m3/d)',
            'cum oil': 'Cum Oil (m3)',
            'oil rate': 'Oil Rate (m3/d)',
            'liquid rate': 'Liquid Rate (m3/d)',
            'tht': 'THT (C)',
            'cht': 'CHT (C)',
            'n2': 'N2 (%)',
            'co2': 'CO2 (%)',
            'gas comp': 'Gas Comp (%)',
            'total gas rate': 'Total Gas Rate (e3m3/d)',       
            'api temp': 'API temp', 
            'api@60f': 'API@60F',
            'source file': 'Source File',
            'unique well identifier': 'Unique Well Identifier',
            'formation': 'Formation'
        }

        # Read and merge processed files
        processed_files = glob.glob(os.path.join(folder_path, 'processed_*.csv'))
        dataframes = []

        for file_path in processed_files:
            try:
                df = pd.read_csv(file_path)
                
                # Create a mapping of existing columns to normalized versions
                normalized_mapping = {}
                for col in df.columns:
                    normalized = normalize_col(col)
                    if normalized in column_mapping:
                        normalized_mapping[col] = column_mapping[normalized]
                
                # Rename columns based on the mapping
                df = df.rename(columns=normalized_mapping)

                # Fix UWI formatting
                uwi_columns = [col for col in df.columns if 'unique well' in col.lower()]
                for uwi_col in uwi_columns:
                    df[uwi_col] = df[uwi_col].apply(fix_uwi)
                    print(f"Sample fixed UWIs from {file_path}:")
                    print(df[uwi_col].head())

                dataframes.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

        if dataframes:
            merged_df = pd.concat(dataframes, ignore_index=True)
            merged_df.fillna(0, inplace=True)
            
            # Define desired columns for final output
            desired_columns = [
                'License', 'Unique Well Identifier', 'Well Name', 'Formation', 'Date', 'Time', 'Flow Time', 
                'Casing Press (kPa)', 'Tubing Press (kPa)', 'Flow Temp (C)', 'Choke Size (mm)', 'Orifice (mm)', 
                'Static Press (kPa)', 'Diff Press (kPa)', 'Meter Temp (C)', 'Total Gas Rate (e3m3/d)', 
                'Total Gas Produced (e3m3)', 'Total Gas Flared (e3m3)', 'Total Gas Pipelined (e3m3)', 
                'Water Gain (m3)', 'Water Cum (m3)', 'Condi Gain (m3)', 'Condi Cum (m3)', 'Total Fluids Gain (m3)', 
                'pH', 'Salinity (% or ppm)', 'WGR (m3/e3m3)', 'CGR (m3/e3m3)', 'LGR (m3/e3m3)', 'GOR (e3m3/m3)',
                'Cum wells to floc tank', 'Abraisives Level Flock Tank (cm)', 'Abraisives Volume Flock Tank (m3)', 
                'Abraisives to Disposal (m3)', 'Gas Rate (Inst) (e3m3/d)', 'BS&W', 'Condi Rate', 'Water Rate', 
                'Measured API', 'API Sample Temp', 'API@60F', 'PL pres', 'PL temp', 'LFTR', 'Gas Incin (e3m3)', 
                'Gas Vented (e3m3)', 'Total Oil Rate (m3/d)', 'Total Water Rate (m3/d)', 'Cum Oil (m3)', 
                'Oil Rate (m3/d)', 'Liquid Rate (m3/d)', 'THT (C)', 'CHT (C)', 'H2S (%, ppm)', 'N2 (%)', 'CO2 (%)', 
                'Gas Comp (%)', 'Source File'
            ]
            
            # Add missing columns
            for col in desired_columns:
                if col not in merged_df.columns:
                    merged_df[col] = ""
            
            # Reorder columns
            merged_df = merged_df[desired_columns]

            merged_output_path = os.path.join(folder_path, 'Skyline_merged.csv')
            merged_df.to_csv(merged_output_path, index=False, encoding='utf-8-sig')
            print(f"Merged file saved to {merged_output_path}")

        # Merge with fix folder and update database
        try:
            skyline_merged_path = os.path.join(folder_path, 'Skyline_merged.csv')
            fix_folder = r'C:\Users\Rita.Fazlyeva\Shell\Groundbirch Team - Spotfire data\Flowback database update'
            fix_files = glob.glob(os.path.join(fix_folder, '*.csv'))
            
            if not fix_files:
                print("No CSV files found in the fix folder.")
            else:
                fix_file_path = fix_files[0]
                df_skyline = pd.read_csv(skyline_merged_path)
                df_fix = pd.read_csv(fix_file_path)

                # Ensure both DataFrames have all desired columns
                for col in desired_columns:
                    if col not in df_skyline.columns:
                        df_skyline[col] = ""
                    if col not in df_fix.columns:
                        df_fix[col] = ""
                
                df_skyline = df_skyline[desired_columns]
                df_fix = df_fix[desired_columns]

                # Concatenate the dataframes
                merged_final = pd.concat([df_skyline, df_fix], ignore_index=True, sort=False)
                
                # Ensure all desired columns exist
                for col in desired_columns:
                    if col not in merged_final.columns:
                        merged_final[col] = ""
                merged_final = merged_final[desired_columns]

                # Update flowback database
                flowback_db_path = os.path.join(fix_folder, 'Flowback_database.csv')
                key_columns = ['Unique Well Identifier', 'Date', 'Time']

                if os.path.exists(flowback_db_path):
                    df_flowback = pd.read_csv(flowback_db_path, low_memory=False)
                    
                    # Ensure all desired columns exist in flowback database
                    for col in desired_columns:
                        if col not in df_flowback.columns:
                            df_flowback[col] = ""
                    df_flowback = df_flowback[desired_columns]

                    # Remove duplicates based on key columns
                    merged_final['merge_key'] = merged_final[key_columns].astype(str).agg('_'.join, axis=1)
                    df_flowback['merge_key'] = df_flowback[key_columns].astype(str).agg('_'.join, axis=1)
                    df_flowback = df_flowback[~df_flowback['merge_key'].isin(merged_final['merge_key'])]
                    df_flowback.drop(columns='merge_key', inplace=True)
                    merged_final.drop(columns='merge_key', inplace=True)
                    updated_db = pd.concat([df_flowback, merged_final], ignore_index=True, sort=False)
                    updated_db = updated_db[desired_columns]
                else:
                    updated_db = merged_final.copy()

                updated_db.to_csv(flowback_db_path, index=False, encoding='utf-8-sig')
                print(f"Flowback database updated at {flowback_db_path}")
                
        except Exception as e:
            print(f"An error occurred during the merge and update process: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

def check_and_move_old_files(folder_path):
    """Check for files not updated in 2 weeks and move them to 'Completed flowback' folder"""
    try:
        # Create the completed flowback folder if it doesn't exist
        completed_folder = os.path.join(folder_path, 'Completed flowback')
        if not os.path.exists(completed_folder):
            os.makedirs(completed_folder)
            print(f"Created folder: {completed_folder}")
        
        # Get current date
        current_date = datetime.datetime.now()
        two_weeks_ago = current_date - datetime.timedelta(weeks=2)
        
        # Find all CSV files (excluding processed and merged files)
        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        moved_files = []
        
        for file_path in csv_files:
            # Skip already processed files, merged files, and problem files
            filename = os.path.basename(file_path)
            if (filename.startswith('processed_') or 
                filename.startswith('Skyline_merged') or 
                filename == 'problem_files.txt'):
                continue
            
            # Get file modification time
            mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            
            # If file hasn't been modified in 2 weeks, move it
            if mod_time < two_weeks_ago:
                destination = os.path.join(completed_folder, filename)
                try:
                    shutil.move(file_path, destination)
                    moved_files.append(filename)
                    print(f"Moved {filename} to Completed flowback folder (last modified: {mod_time.strftime('%Y-%m-%d')})")
                except Exception as e:
                    print(f"Error moving {filename}: {e}")
        
        if moved_files:
            print(f"\nMoved {len(moved_files)} files to 'Completed flowback' folder:")
            for file in moved_files:
                print(f"  - {file}")
        else:
            print("\nNo files older than 2 weeks found to move.")
            
        return moved_files
        
    except Exception as e:
        print(f"Error in check_and_move_old_files: {e}")
        return []

# Run the function
process_files_in_folder(folder_path)
check_and_move_old_files(folder_path)