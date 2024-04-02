import pandas as pd

class DataProcessor:
    def read_from_csv(self, file):
        # Read data from CSV file
        return pd.read_csv(file)

    def process_data(self, data):
        # Placeholder method to be implemented by subclasses
        raise NotImplementedError("Subclasses must implement process_data method")

    def write_to_csv(self, data, output_file):
        # Write processed data to CSV file
        data.to_csv(output_file, index=False)

class BlockLoadPhase1(DataProcessor):
    def process_data(self, data, rules_to_be_applied, rules):
        # Process block load data for phase 1 meter


        #rules_to_be_applied formatting
        rule_conditions = {
            1: "import_VAh < 0",
            2: "import_Wh < 0",
            3: "export_VAh < 0",
            4: "export_Wh < 0"
            # Add more rule IDs and conditions as needed
        }
        # column, operator, value = rule_conditions.get(1).split()
        # Combine conditions into a single condition string
        # combined_condition = " and ".join(f"({condition})" for condition in rule_conditions.values())
        mask = pd.eval(rule_conditions.get(1), engine='python', local_dict={col: data[col] for col in data.columns})
        print(mask)
        # Apply the combined condition to mark anomalies
        data["Anomaly"] = "No"  # Assuming no anomaly initially
        # Add is_valid and vee_rules[]

        if mask is not None:
            data.loc[mask, 'Anomaly'] = 'Negative Validation Fail.' # description of vee rule will be appended.
        #data.loc[data.query(combined_condition).index, 'Anomaly'] = 'Yes'
        # data["Anomaly"] = "No"  # Assuming no anomaly initially
         


        # data.loc[data['import_VAh'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['import_Wh'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['export_VAh'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['export_Wh'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['avg_current'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['avg_voltage'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['cumm_export_Wh'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['cumm_export_VAh'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['cumm_import_VAh'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['cumm_import_Wh'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['daily_cumm_active_energy_exp'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['daily_cumm_active_energy_imp'] < 0, 'Anomaly'] = 'Yes'
        # data.loc[data['daily_cumm_apparent_energy_imp'] < 0, 'Anomaly'] = 'Yes'

        return data

class BlockLoadPhase3(DataProcessor):
    def process_data(self, data):
        # Process block load data for phase 3 meter
        # Example processing: Multiply values in the 'import_Wh' column by 2
        data['import_Wh'] *= 2
        return data

class BlockLoadLTCT(DataProcessor):
    def process_data(self, data):
        data['import_Wh'] *= 3
        return data
# Factory method to create instances of data processors
def create_data_processor(data_type):
    if data_type == 'BlockLoadPhase1':
        return BlockLoadPhase1()
    elif data_type == 'BlockLoadPhase3':
        return BlockLoadPhase3()
    elif data_type == 'BlockLoadLTCT':
        return BlockLoadLTCT();
    # Add more conditions for other data types

