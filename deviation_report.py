import os
import pandas as pd
import numpy as np

# Base directory paths
base_path = os.getcwd()  # Set to the current working directory or customize
source_files_path = os.path.join(base_path, "source_files")
simulation_output_path = os.path.join(base_path, "simulation_output")
output_path = os.path.join(base_path, "reports")

# Target ISO code and reference year/id
iso_code3 = "HRV"
refy = 2015
ref_primary_id = 0

# Load mapping table
mapping = pd.read_csv(os.path.join(source_files_path, "mapping.csv"))

# Load raw simulation data
slt = pd.read_csv(os.path.join(simulation_output_path, "sisepuede_results_sisepuede_run_2024.csv"))

# Estimate emission totals for the initial year
slt['Year'] = slt['time_period'] + 2015
print("SLT DataFrame sample after adding 'Year' column:")
print(slt.head())

for i in range(len(mapping)):
    vars_ = mapping.loc[i, 'Vars'].split(":")
    try:
        if len(vars_) > 1:
            mapping.loc[i, 'simulation'] = slt[(slt['primary_id'] == ref_primary_id) & (slt['Year'] == refy)][vars_].sum(axis=1).sum()
        else:
            mapping.loc[i, 'simulation'] = slt[(slt['primary_id'] == ref_primary_id) & (slt['Year'] == refy)][vars_[0]].sum()
    except KeyError as e:
        print(f"Warning: Column(s) {vars_} not found in simulation data. Error: {e}")

# Debugging the mapping DataFrame after adding 'simulation' values
print("Mapping DataFrame sample after simulation calculations:")
print(mapping.head())

# Load edgar data and filter by iso_code3
edgar = pd.read_csv(os.path.join(source_files_path, "CSC-GHG_emissions-April2024_to_calibrate.csv"), encoding='latin1')
print("Edgar DataFrame sample before filtering:")
print(edgar.head())

edgar = edgar[edgar['Code'] == iso_code3]  # Filter by iso_code3
print(f"Edgar DataFrame after filtering for iso_code3 = {iso_code3}:")
print(edgar.head())

edgar['Edgar_Class'] = edgar['CSC Subsector'] + ":" + edgar['Gas']
print("Unique Edgar_Class values in edgar after adding 'Edgar_Class':")
print(edgar['Edgar_Class'].unique())

# Melt edgar data
id_varsEd = ["Edgar_Class"]
measure_vars_Ed = [col for col in edgar.columns if col.isdigit()]  # Select year columns
edgar = pd.melt(edgar, id_vars=id_varsEd, value_vars=measure_vars_Ed, var_name="Year", value_name="Edgar_value")
edgar['Year'] = edgar['Year'].astype(int)
edgar = edgar[edgar['Year'] == refy][["Edgar_Class", "Edgar_value"]]
print(f"Edgar DataFrame after melting and filtering for Year = {refy}:")
print(edgar.head())

# Debugging unique Edgar_Class values before merging
print("Unique Edgar_Class values in mapping before merging:")
print(mapping['Edgar_Class'].unique())

# Merge both and generate reports
report_1 = mapping.groupby(['Subsector', 'Edgar_Class'])['simulation'].sum().reset_index()
report_1 = pd.merge(report_1, edgar, on="Edgar_Class", how="left", indicator=True)
print("Report 1 after merging with edgar:")
print(report_1.head())
print("Merge indicator value counts in report_1:")
print(report_1['_merge'].value_counts())

# Calculate differences and save reports
report_1['diff'] = (report_1['simulation'] - report_1['Edgar_value']) / report_1['Edgar_value']
report_1['Year'] = refy
report_1.to_csv(os.path.join(base_path, "reports/detailed_diff_report.csv"), index=False)

report_2 = report_1.groupby('Subsector').agg({'simulation': 'sum', 'Edgar_value': 'sum'}).reset_index()
report_2['diff'] = (report_2['simulation'] - report_2['Edgar_value']) / report_2['Edgar_value']
report_2['Year'] = refy
os.makedirs(output_path, exist_ok=True)
report_2.to_csv(os.path.join(output_path, "sector_diff_report.csv"), index=False)

print("Report generation completed.")
