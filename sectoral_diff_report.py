import os
import pandas as pd


class SectoralDiffReport:
    
    def __init__(self, misc_dir_path, iso_alpha_3, init_year, ref_year=2015, ref_primary_id=0):
        """
        Initialize the utility class with the given parameters.
        Args:
            misc_dir_path (str): The directory path where miscellaneous files are stored.
            iso_alpha_3 (str): The ISO 3166-1 alpha-3 country code.
            init_year (int): The initial year for the simulation.
            ref_year (int, optional): The reference year. Defaults to 2015.
            ref_primary_id (int, optional): The reference primary ID. Defaults to 0.
        Attributes:
            iso_alpha_3 (str): The ISO 3166-1 alpha-3 country code.
            ref_year (int): The reference year.
            ref_primary_id (int): The reference primary ID.
            mapping_table_path (str): The path to the mapping table CSV file.
            init_year (int): The initial year for the simulation.
            edga_file_path (str): The path to the EDGAR data file containing ground truth data.
            misc_dir_path (str): The directory path where miscellaneous files are stored.
            report_type (str): The type of report, default is 'all-sectors'.
        """
        
        # Set up variables
        self.iso_alpha_3 = iso_alpha_3
        self.ref_year = ref_year # Reference year
        self.ref_primary_id = ref_primary_id # Reference primary_id
        self.mapping_table_path = os.path.join(misc_dir_path, 'mapping.csv') # Mapping table path
        self.init_year = init_year # Simulation's start year
        self.edga_file_path = os.path.join(misc_dir_path, 'CSC-GHG_emissions-April2024_to_calibrate.csv') # Edgar data file path containing ground truth data
        self.misc_dir_path = misc_dir_path
        self.report_type = 'all-sectors'

    def load_mapping_table(self):
        # Load mapping tables
        mapping_df = pd.read_csv(self.mapping_table_path)
        
        return mapping_df
    
    def load_simulation_output_data(self, simulation_df):

        simulation_df_filtered = simulation_df.copy()

        # Add a year column to the simulation data
        simulation_df_filtered['year'] = simulation_df_filtered['time_period'] + self.init_year

        # Filter the simulation data to the reference year and reference primary id
        simulation_df_filtered = simulation_df_filtered[(simulation_df_filtered['year'] == self.ref_year) & (simulation_df_filtered['primary_id'] == self.ref_primary_id)]
 
        return simulation_df_filtered
    
    def edgar_data_etl(self):
        # Load Edgar data
        edgar_df = pd.read_csv(self.edga_file_path, encoding='latin1')

        # Filter Edgar data to the reference year and reference primary id
        edgar_df = edgar_df[edgar_df['Code'] == self.iso_alpha_3].reset_index(drop=True)

        # Create Edgar_Class column by combining Subsector and Gas columns
        edgar_df['Edgar_Class'] = edgar_df['CSC Subsector'] + ':' + edgar_df['Gas']

        # Specify the id_vars (columns to keep) and value_vars (columns to unpivot)
        
        id_vars = ['Edgar_Class']
        value_vars = [str(self.ref_year)]

        # Melt the DataFrame
        edgar_df_long = edgar_df.melt(id_vars=id_vars, value_vars=value_vars, 
                        var_name='Year', value_name='Edgar_Values')

        # Convert the 'year' column to integer type
        edgar_df_long['Year'] = edgar_df_long['Year'].astype(int)
        
        return edgar_df_long

    
    def calculate_ssp_emission_totals(self, simulation_df, mapping_df):
        
        # Create detailed report df from mapping df
        detailed_report_draft_df = mapping_df.copy()
        
        # Add a new column to store total emissions
        detailed_report_draft_df['Simulation_Values'] = 0  # Initialize with zeros

        # Create a set of all column names in simulation_df for quick lookup
        simulation_df_cols = set(simulation_df.columns)

        # Create a set to store all missing variable names
        missing_variables = set()

        # Iterate through each row in detailed_report_draft_df
        for index, row in detailed_report_draft_df.iterrows():
            vars_list = row['Vars'].split(':')  # Split Vars column into variable names

            # Check which variable names are missing in simulation_df
            missing_in_row = [var for var in vars_list if var not in simulation_df_cols]
            missing_variables.update(missing_in_row)  # Add missing variables to the set

            # Filter the columns in simulation_df that match the variable names
            matching_columns = [col for col in vars_list if col in simulation_df_cols]

            if matching_columns:
                # Sum the matching columns across all rows in simulation_df
                subsector_total_emissions = simulation_df[matching_columns].sum().sum()
            else:
                subsector_total_emissions = 0  # No matching columns found

            # Update the simulation_values column in detailed_report_draft_df
            detailed_report_draft_df.at[index, 'Simulation_Values'] = subsector_total_emissions

        # Print missing variable names, if any
        if missing_variables:
            print("The following variables from Vars are not present in simulation_df:")
            for var in missing_variables:
                print(var)
        else:
            print("All variables from Vars are present in simulation_df.")

        # Returns the updated detailed_report_draft_df
        return detailed_report_draft_df
    
    def generate_detailed_diff_report(self, detailed_report_draft_df, edgar_df):

        detailed_diff_report = detailed_report_draft_df.copy()

        # Group by Subsector and Edgar_Class and aggregate the Simulation_Values to match Edgar_Values format
        detailed_diff_report_agg = detailed_diff_report.groupby(['Subsector', 'Edgar_Class'])['Simulation_Values'].sum().reset_index()

        # Merge the aggregated DataFrame with the Edgar data
        detailed_diff_report_merge = pd.merge(detailed_diff_report_agg, edgar_df, how='left', left_on='Edgar_Class', right_on='Edgar_Class')

        # Calculate the difference between Simulation_Values and Edgar_Values
        detailed_diff_report_merge['diff'] = (detailed_diff_report_merge['Simulation_Values'] - detailed_diff_report_merge['Edgar_Values']) / detailed_diff_report_merge['Edgar_Values']

        # Reset Year column to ref year to avoid NaN values
        detailed_diff_report_merge['Year'] = self.ref_year

        detailed_diff_report_complete = detailed_diff_report_merge[['Year', 'Subsector', 'Edgar_Class', 'Simulation_Values', 'Edgar_Values', 'diff']]
        
        return detailed_diff_report_complete
    
    def generate_subsector_diff_report(self, detailed_diff_report_complete):
        
        # Group by Subsector and calculate the sum of the Simulation_Values and Edgar_Values
        subsector_diff_report = detailed_diff_report_complete.groupby('Subsector')[['Simulation_Values', 'Edgar_Values']].sum().reset_index()

        # Calculate the difference between Simulation_Values and Edgar_Values
        subsector_diff_report['diff'] = (subsector_diff_report['Simulation_Values'] - subsector_diff_report['Edgar_Values']) / subsector_diff_report['Edgar_Values']

        # Reset Year column to ref year to avoid NaN values
        subsector_diff_report['Year'] = self.ref_year

        # Reorder columns
        subsector_diff_report = subsector_diff_report[['Year', 'Subsector', 'Simulation_Values', 'Edgar_Values', 'diff']] 

        return subsector_diff_report
    
    def generate_diff_reports(self, simulation_df):

        mapping_df = self.load_mapping_table()
        simulation_df_filtered = self.load_simulation_output_data(simulation_df)
        edgar_df = self.edgar_data_etl()
        detailed_report_draft_df = self.calculate_ssp_emission_totals(simulation_df_filtered, mapping_df)
        detailed_diff_report_complete = self.generate_detailed_diff_report(detailed_report_draft_df, edgar_df)
        subsector_diff_report = self.generate_subsector_diff_report(detailed_diff_report_complete)

        detailed_diff_report_complete.to_csv(os.path.join(self.misc_dir_path, f'detailed_diff_report_{self.report_type}.csv'), index=False)
        subsector_diff_report.to_csv(os.path.join(self.misc_dir_path, f'subsector_diff_report_{self.report_type}.csv'), index=False)

        return detailed_diff_report_complete, subsector_diff_report