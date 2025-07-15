import os
import glob
import pandas as pd
import numpy as np
import re

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
    col = str(col).strip().lower()
    col = re.sub(r'\s+', ' ', col)
    col = re.sub(r'\s*\(.*?\)', '', col)  # Remove units in parentheses
    col = col.replace('%', 'percent').replace('°', 'deg')
    return col

def process_files_in_folder(folder_path):
    processed_files = []
    dataframes = []
    problem_files = []  # List to collect files missing Unique Well Information
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
                        normalized_column_names = [normalize_col(col) for col in column_names]
                        processed_data = [column_names]

                        for line in lines[header_row_index + 1:]:
                            row_data = re.split(r'[,\t]', line.strip())
                            if len(row_data) == len(column_names):
                                processed_data.append(row_data)

                        df = pd.DataFrame(processed_data[1:], columns=normalized_column_names)
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
                    df.columns = [normalize_col(col) for col in df.columns]

                df = df.loc[:, (df.columns != "") & (df.columns != "comments")]

                columns_to_interpolate = [
                    'static press', 'diff press', 'meter temp', 'total gas rate',
                    'total gas produced', 'total gas flared', 'total gas pipelined', 'water gain',
                    'water cum', 'condi gain', 'condi cum', 'total fluids gain', 'ph', 'salinity',
                    'wgr', 'cgr', 'lgr', 'cum wells to floc tank', 'abraisives level flock tank (cm)',
                    'abraisives volume flock tank', 'abraisives to disposal', 'gas rate (inst)', 'bs&w',
                    'condi rate', 'water rate', 'api sample temp', 'api @60f', 
                    'lftr', 'pipeline pressure', 'pipeline temp'
                ]

                for col in columns_to_interpolate:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        first_nonzero_index = df[df[col] != 0].index.min()
                        if pd.notna(first_nonzero_index):
                            mask = df.index >= first_nonzero_index
                            interpolated = df[col].copy()
                            interpolated[mask] = interpolated[mask].interpolate(method='linear', limit_direction='forward')
                            df[col] = interpolated

                vt_cols = ['total gas produced', 'condi cum', 'water cum']
                if all(col in df.columns for col in vt_cols):
                    df = df.loc[~(df[vt_cols] == df[vt_cols].shift()).all(axis=1)]

                for col in ['ph', 'salinity', 'measured api', 'choke size']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].where(df[col] > 0, np.nan)
                        df[col] = df[col].ffill().bfill()

                if 'well name' not in df.columns:
                    df.insert(0, 'well name', well_name)
                if 'unique well identifier' not in df.columns:
                    df.insert(1, 'unique well identifier', unique_well_id)
                if 'formation' not in df.columns:
                    df.insert(2, 'formation', formation)

                output_file_path = os.path.join(folder_path, 'processed_' + os.path.basename(file_path))
                if os.path.exists(output_file_path):
                    os.remove(output_file_path)
                df['source file'] = os.path.basename(file_path)
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
            'THT': 'THT (C)',
            'CHT': 'CHT (C)',
            'N2': 'N2 (%)',
            'CO2': 'CO2 (%)',
            'gas comp': 'Gas Comp (%)',
            'total gas rate': 'Total Gas Rate (e3m3/d)',       
            'water rate (m3/d)': 'Water Rate (m3/d)',
            'api temp':'API temp', 
            'api@60f': 'API@60F',
            'source file': 'Source File'
        }

        processed_files = glob.glob(os.path.join(folder_path, 'processed_*.csv'))
        dataframes = []

        for file_path in processed_files:
            try:
                df = pd.read_csv(file_path)
                # Normalize columns before mapping
                df.columns = [normalize_col(c) for c in df.columns]
                # Map to standard names
                df = df.rename(columns=column_mapping)

                # Fix UWI formatting
                if 'Unique Well Identifier' in df.columns:
                    df['Unique Well Identifier'] = df['Unique Well Identifier'].apply(fix_uwi)
                    print(f"Sample fixed UWIs from {file_path}:")
                    print(df['Unique Well Identifier'].head())

                dataframes.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

        if dataframes:
            merged_df = pd.concat(dataframes, ignore_index=True)
            merged_df.fillna(0, inplace=True)
            desired_columns = [
                'License', 'Unique Well Identifier', 'Well Name','Formation', 'Date', 'Time', 'Flow Time', 'Casing Press (kPa)', 'Tubing Press (kPa)',
                'Flow Temp (C)', 'Choke Size (mm)', 'Orifice (mm)', 'Static Press (kPa)', 'Diff Press (kPa)', 'Meter Temp (C)', 'Total Gas Rate (e3m3/d)', 'Total Gas Produced (e3m3)',
                'Total Gas Flared (e3m3)', 'Total Gas Pipelined (e3m3)', 'Water Gain (m3)', 'Water Cum (m3)', 'Condi Gain (m3)', 'Condi Cum (m3)',
                'Total Fluids Gain (m3)', 'pH', 'Salinity (% or ppm)', 'WGR (m3/e3m3)', 'CGR (m3/e3m3)', 'LGR (m3/e3m3)','GOR (e3m3/m3)','Cum wells to floc tank',
                'Abraisives Level Flock Tank (cm)', 'Abraisives Volume Flock Tank (m3)', 'Abraisives to Disposal (m3)', 'Gas Rate (Inst) (e3m3/d)',
                'BS&W', 'Condi Rate', 'Water Rate', 'Measured API', 'API Sample Temp', 'API @60F', 'PL pres', 'PL temp', 'API', 'API temp', 'API@60F', 'Sand(%) ', 'LFTR', 'PL pres', 'PL temp',
                'Gas Incin (e3m3)', 'Gas Vented (e3m3)', 'Total Oil Rate (m3/d)', 'Total Condi Rate (m3/d)', 'Total Water Rate (m3/d)', 'Cum Oil (m3)', 'Oil Rate (m3/d)',
                'Liquid Rate (m3/d)', 'THT (C)', 'CHT (C)', 'H2S (%, ppm)', 'N2 (%)', 'CO2 (%)', 'Injected/Remaining Water (m3)',
                'Gas Comp (%)', 'Comm', 'Source File', 'Time_diff_WR', 'Start_depth (m)', 'End_depth (m)','Date & Time'
            ]
            for col in desired_columns:
                if col not in merged_df.columns:
                    merged_df[col] = ""
            merged_df = merged_df[desired_columns]

            merged_output_path = os.path.join(folder_path, 'Skyline_merged.csv')
            merged_df.to_csv(merged_output_path, index=False, encoding='utf-8-sig')
            print(f"Merged file saved to {merged_output_path}")

            # --- Merge Skyline_merged.csv with a file from 'fix' folder and update Flowback database ---
        try:
            skyline_merged_path = os.path.join(folder_path, 'Skyline_merged.csv')
            fix_folder = r'C:\Users\Rita.Fazlyeva\Desktop\Database for Flowback\AllCSV\Test\fix'
            fix_files = glob.glob(os.path.join(fix_folder, '*.csv'))
            if not fix_files:
                print("No CSV files found in the fix folder.")
            else:
                fix_file_path = fix_files[0]
                df_skyline = pd.read_csv(skyline_merged_path)
                df_fix = pd.read_csv(fix_file_path)

                # Normalize columns for robust mapping
                df_skyline.columns = [normalize_col(c) for c in df_skyline.columns]
                df_fix.columns = [normalize_col(c) for c in df_fix.columns]
                norm_desired = [normalize_col(c) for c in desired_columns]
                norm_to_desired = {normalize_col(c): c for c in desired_columns}

                # Ensure both DataFrames have all desired columns
                for col in norm_desired:
                    if col not in df_skyline.columns:
                        df_skyline[col] = ""
                    if col not in df_fix.columns:
                        df_fix[col] = ""
                df_skyline = df_skyline[norm_desired]
                df_fix = df_fix[norm_desired]

                # Concatenate rows by matching columns (header names)
                merged_final = pd.concat([df_skyline, df_fix], ignore_index=True, sort=False)
                for col in norm_desired:
                    if col not in merged_final.columns:
                        merged_final[col] = ""
                merged_final = merged_final[norm_desired]
                merged_final.columns = [norm_to_desired[c] for c in merged_final.columns]

                flowback_db_path = os.path.join(fix_folder, 'Flowback_database.csv')
                key_columns = ['Unique Well Identifier', 'Date', 'Time']  # adjust as needed
                norm_key_columns = [normalize_col(c) for c in key_columns]

                if os.path.exists(flowback_db_path):
                    df_flowback = pd.read_csv(flowback_db_path, low_memory=False)
                    df_flowback.columns = [normalize_col(c) for c in df_flowback.columns]
                    for col in norm_desired:
                        if col not in df_flowback.columns:
                            df_flowback[col] = ""
                    df_flowback = df_flowback[norm_desired]

                    # Merge logic with normalized columns
                    merged_final['merge_key'] = merged_final[norm_key_columns].astype(str).agg('_'.join, axis=1)
                    df_flowback['merge_key'] = df_flowback[norm_key_columns].astype(str).agg('_'.join, axis=1)
                    df_flowback = df_flowback[~df_flowback['merge_key'].isin(merged_final['merge_key'])]
                    df_flowback.drop(columns='merge_key', inplace=True)
                    merged_final.drop(columns='merge_key', inplace=True)
                    updated_db = pd.concat([df_flowback, merged_final], ignore_index=True, sort=False)
                    updated_db = updated_db[norm_desired]
                    updated_db.columns = [norm_to_desired[c] for c in updated_db.columns]
                else:
                    updated_db = merged_final.copy()

                updated_db.to_csv(flowback_db_path, index=False, encoding='utf-8-sig')
                print(f"Flowback database updated at {flowback_db_path}")
        except Exception as e:
            print(f"An error occurred during the merge and update process: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

# Run the function
process_files_in_folder(folder_path)
